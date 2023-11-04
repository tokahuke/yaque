use std::fs::*;
use std::io::{self};
use std::path::{Path, PathBuf};

use crate::header::Header;
use crate::state::{QueueState, QueueStatePersistence};
use crate::sync::{FileGuard, SyncFollower};
use crate::version::check_queue_version;

use super::try_acquire_recv_lock;
use super::{segment_filename, HEADER_EOF};

/// An [`Iterator`] that iterates over the elements of the queue, until it hts
/// the end for the first time. Use this structure instead of
/// [`crate::Receiver`] if you just need to read the data stored in a queue.
///
/// Three good reasons for this are:
///
/// 1. The API is synchronous, since there is no need to wait for new elements.
/// 2. There is no transactional mechanism involved, since there is no need for
/// one and, because of this,
/// 3. Elements are not buffered in memory, as opposed to what
/// [`crate::Receiver::recv_batch`] does.
///
/// And you also get some extra percents of performance from a simpler
/// implementation. Don't pay for what you don't use!
pub struct QueueIter {
    _file_guard: FileGuard,
    base: PathBuf,
    state: QueueState,
    sync_follower: SyncFollower,
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
        let mut sync_follower = SyncFollower::open(segment_filename(base.as_ref(), state.segment))?;
        sync_follower.seek(io::SeekFrom::Start(state.position))?;

        log::trace!("last segment opened fo reading");

        Ok(QueueIter {
            _file_guard: file_guard,
            state,
            base: PathBuf::from(base.as_ref()),
            sync_follower,
        })
    }

    /// Puts the queue in another position in another segment. This forcibly
    /// discards the old tail follower and fetches a fresh new one, so be
    /// careful.
    fn advance_segment(&mut self) -> io::Result<()> {
        let current_segment = self.state.segment;
        self.state.advance_segment();
        let next_segment = self.state.segment;

        log::debug!(
            "advanced segment from {:?} to {:?}",
            current_segment,
            next_segment
        );

        log::debug!("opening segment {}", next_segment);
        self.sync_follower = SyncFollower::open(segment_filename(&self.base, next_segment))?;

        Ok(())
    }

    /// Reads the header. This operation is atomic.
    fn read_header(&mut self) -> io::Result<Header> {
        // Read header:
        let mut header = [0; 4];
        self.sync_follower.read_exact(&mut header)?;

        // If the header is EOF, advance segment:
        if header == HEADER_EOF {
            log::trace!("got EOF header. Advancing...");
            self.advance_segment()?;

            // Re-read the header:
            log::trace!("re-reading new header from new file");
            self.sync_follower.read_exact(&mut header)?;
        }

        // Now, you set the header!
        let decoded = Header::decode(header);

        log::trace!("got header {:?} (read {} bytes)", header, decoded.len());

        Ok(decoded)
    }

    /// Reads one element from the queue.
    fn read_one(&mut self) -> io::Result<Vec<u8>> {
        // Get the length:
        let header = self.read_header()?;

        // With the length, read the data:
        let mut data = vec![0; header.len() as usize];
        self.sync_follower
            .read_exact(&mut data)
            .expect("poisoned queue");

        Ok(data)
    }
}

impl Iterator for QueueIter {
    type Item = io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<io::Result<Vec<u8>>> {
        match self.read_one() {
            Ok(item) => Some(Ok(item)),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                log::trace!("got interrupted by eof");
                None
            }
            Err(err) => Some(Err(err)),
        }
    }
}
