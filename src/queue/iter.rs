use futures::{stream, FutureExt, Stream};
use std::fs::*;
use std::io::{self};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::header::Header;
use crate::state::QueueStatePersistence;
use crate::sync::{FileGuard, TailFollower};
use crate::version::check_queue_version;

use super::try_acquire_recv_lock;
use super::{segment_filename, HEADER_EOF};

/// The receiver part of the queue. This part is asynchronous and therefore
/// needs an executor that will the poll the futures to completion.
pub struct QueueIter {
    _file_guard: FileGuard,
    tail_follower: TailFollower,
}

impl QueueIter {
    /// Opens a queue for reading. The access will be exclusive, based on the
    /// existence of the temporary file `recv.lock` inside the queue folder.
    ///
    /// # Errors
    ///
    /// This function will return an IO error if the queue is already in use for
    /// receiving, which is indicated by a lock file. Also, any other IO error
    /// encountered while opening will be sent.
    ///
    /// # Panics
    ///
    /// This function will panic if it is not able to set up the notification
    /// handler to watch for file changes.
    pub fn open<P: AsRef<Path>>(base: P) -> io::Result<QueueIter> {
        // Guarantee that the queue exists:
        create_dir_all(base.as_ref())?;

        log::trace!("created queue directory");

        // Versioning stuff (this should be lightning-fast. Therefore, shameless block):
        check_queue_version(base.as_ref())?;

        // Acquire guard and state:
        let file_guard = try_acquire_recv_lock(base.as_ref())?;
        let mut persistence = QueueStatePersistence::new();
        let state = persistence.open(base.as_ref())?;

        log::trace!("receiver lock acquired. Iter state now is {:?}", state);

        // Put the needle on the groove (oh! the 70's):
        let mut tail_follower = TailFollower::open(segment_filename(base.as_ref(), state.segment))?;
        tail_follower.seek(io::SeekFrom::Start(state.position))?;

        log::trace!("last segment opened fo reading");

        Ok(QueueIter {
            _file_guard: file_guard,
            tail_follower,
        })
    }

    /// Reads the header. This operation is atomic.
    async fn read_header(&mut self) -> io::Result<Header> {
        // Read header:
        let mut header = [0; 4];
        self.tail_follower.read_exact(&mut header).await?;

        // If the header is EOF, advance segment:
        if header == HEADER_EOF {
            log::trace!("got EOF header. Advancing...");

            // Re-read the header:
            log::trace!("re-reading new header from new file");
            self.tail_follower.read_exact(&mut header).await?;
        }

        // Now, you set the header!
        let decoded = Header::decode(header);

        log::trace!("got header {:?} (read {} bytes)", header, decoded.len());

        Ok(decoded)
    }

    /// Reads one element from the queue, inevitably advancing the file reader.
    /// Instead of returning the element, this function puts it in the "read and
    /// unused" queue to be used later. This enables us to construct "atomic in
    /// async context" guarantees for the higher level functions. The ideia is to
    /// _drain the queue_ only after the last `.await` in the block.
    ///
    /// This operation is also itlsef atomic. If the returned future is not
    /// polled to completion, as, e.g., when calling `select`, the operation
    /// will count as not done.
    async fn read_one(&mut self) -> io::Result<Vec<u8>> {
        // Get the length:
        let header = self.read_header().await?;

        // With the length, read the data:
        let mut data = vec![0; header.len() as usize];
        self.tail_follower
            .read_exact(&mut data)
            .await
            .expect("poisoned queue");

        Ok(data)
    }
}

impl Iterator for QueueIter {
    type Item = io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<io::Result<Vec<u8>>> {
        self.read_one().now_or_never()
    }
}

pub struct QueueStream {
    stream: Pin<Box<dyn Stream<Item = io::Result<Vec<u8>>>>>,
}

impl QueueStream {
    pub fn open<P: AsRef<Path>>(base: P) -> io::Result<QueueStream> {
        let iter = QueueIter::open(base)?;
        let stream = stream::unfold(iter, |mut iter| async move {
            let next = iter.read_one().await;
            Some((next, iter))
        });

        Ok(QueueStream {
            stream: Box::pin(stream),
        })
    }
}

impl Stream for QueueStream {
    type Item = io::Result<Vec<u8>>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Vec<u8>>>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}
