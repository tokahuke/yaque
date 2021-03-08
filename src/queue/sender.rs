use std::fs::*;
use std::io::{self, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use crate::error::TrySendError;
use crate::header::Header;
use crate::state::QueueState;
use crate::sync::{DeletionEvent, FileGuard};

use super::{segment_filename, HEADER_EOF};

/// The name of the sender lock in the queue folder.
pub(crate) fn send_lock_filename<P: AsRef<Path>>(base: P) -> PathBuf {
    base.as_ref().join("send.lock")
}

/// Tries to acquire the sender lock for a queue.
pub(crate) fn try_acquire_send_lock<P: AsRef<Path>>(base: P) -> io::Result<FileGuard> {
    FileGuard::try_lock(send_lock_filename(base.as_ref()))?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            format!(
                "queue `{}` sender side already in use",
                base.as_ref().to_string_lossy()
            ),
        )
    })
}

/// Acquire the sender lock for a queue, awaiting if locked.
pub(crate) async fn acquire_send_lock<P: AsRef<Path>>(base: P) -> io::Result<FileGuard> {
    FileGuard::lock(send_lock_filename(base.as_ref())).await
}

/// Non-recursively get the directory size of a given path.
fn get_dir_size<P: AsRef<Path>>(base: P) -> io::Result<u64> {
    let mut total = 0;

    for dir_entry in read_dir(base.as_ref())? {
        let dir_entry = dir_entry?;
        total += dir_entry.metadata()?.len();
    }

    Ok(total)
}

pub struct SenderBuilder {
    /// The segment size in bytes that will trigger a new segment to be created. Segments an be
    /// bigger than this to accomodate the last element, but nothing beyond that (each segment
    /// must store at least one element).
    ///
    /// Default value: 4MB
    max_segment_size: NonZeroU64,

    /// The queue size that will block the sender from creating a new segment (until the receiver
    /// catches up, deleting old segments). The queue can get bigger than that, but only to
    /// accomodate the last segment (the queue must have at least one segment). Set this to `None`
    /// to create an unbounded queue.
    ///
    /// Small detail: "queue size" is defined here as the total size of the base directory
    /// (non-recursive). Therefore, metadata is included in the queue size as well as any spurious
    /// files that may appear (who knows?) in the base folder. Sub-folders, however, don't count.
    ///
    /// Default value: None
    max_queue_size: Option<NonZeroU64>,
}

impl Default for SenderBuilder {
    fn default() -> SenderBuilder {
        SenderBuilder {
            max_segment_size: NonZeroU64::new(1024 * 1024 * 4).expect("impossible"), // 4MB
            max_queue_size: None,
        }
    }
}

impl SenderBuilder {
    /// Creates a new sender builder. Finish build it by invoking [`SenderBuilder::open`].
    pub fn new() -> SenderBuilder {
        SenderBuilder::default()
    }

    /// The segment size in bytes that will trigger a new segment to be created. Segments an be
    /// bigger than this to accomodate the last element, but nothing beyond that (each segment
    /// must store at least one element).
    ///
    /// Default value: 4MB
    ///
    /// # Panics
    ///
    /// This function panics if `size` is zero.
    pub fn max_segment_size(mut self, size: u64) -> SenderBuilder {
        let size = NonZeroU64::new(size).expect("got max_segment_size=0");
        self.max_segment_size = size;
        self
    }

    /// The queue size that will block the sender from creating a new segment (until the receiver
    /// catches up, deleting old segments). The queue can get bigger than that, but only to
    /// accomodate the last segment (the queue must have at least one segment). Set this to `None`
    /// to create an unbounded queue.
    ///
    /// Small detail: "queue size" is defined here as the total size of the base directory
    /// (non-recursive). Therefore, metadata is included in the queue size as well as any spurious
    /// files that may appear (who knows?) in the base folder. Sub-folders, however, don't count.
    ///
    /// Default value: None
    ///
    /// # Panics
    ///
    /// This function panics if `size` is zero.
    pub fn max_queue_size(mut self, size: Option<u64>) -> SenderBuilder {
        let size = size.map(|s| NonZeroU64::new(s).expect("got max_queue_size=0"));
        self.max_queue_size = size;
        self
    }

    /// Opens a queue on a folder indicated by the `base` path for sending. The
    /// folder will be created if it does not already exist.
    ///
    /// # Errors
    ///
    /// This function will return an IO error if the queue is already in use for
    /// sending, which is indicated by a lock file. Also, any other IO error
    /// encountered while opening will be sent.
    pub fn open<P: AsRef<Path>>(self, base: P) -> io::Result<Sender> {
        // Guarantee that the queue exists:
        create_dir_all(base.as_ref())?;

        log::trace!("created queue directory");

        // Acquire lock and guess statestate:
        let file_guard = try_acquire_send_lock(base.as_ref())?;
        let state = QueueState::for_send_metadata(base.as_ref())?;

        log::trace!("sender lock acquired. Sender state now is {:?}", state);

        // See the docs on OpenOptions::append for why the BufWriter here.
        let file = io::BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(segment_filename(base.as_ref(), state.segment))?,
        );

        log::trace!("last segment opened for appending");

        Ok(Sender {
            max_segment_size: self.max_segment_size,
            max_queue_size: self.max_queue_size,
            _file_guard: file_guard,
            file,
            state,
            deletion_stream: None,
            base: PathBuf::from(base.as_ref()),
        })
    }
}

/// The sender part of the queue. This part is lock-free and therefore can be
/// used outside an asynchronous context.
pub struct Sender {
    max_segment_size: NonZeroU64,
    max_queue_size: Option<NonZeroU64>,
    _file_guard: FileGuard,
    file: io::BufWriter<File>,
    state: QueueState,
    deletion_stream: Option<DeletionEvent>, // lazy inited!
    base: PathBuf,
}

impl Sender {
    /// Opens a queue on a folder indicated by the `base` path for sending. The
    /// folder will be created if it does not already exist.
    ///
    /// # Errors
    ///
    /// This function will return an IO error if the queue is already in use for
    /// sending, which is indicated by a lock file. Also, any other IO error
    /// encountered while opening will be sent.
    pub fn open<P: AsRef<Path>>(base: P) -> io::Result<Sender> {
        SenderBuilder::default().open(base)
    }

    /// Saves the sender queue state. You do not need to use method in most
    /// circumstances, since it is automatically done on drop (yes, it will be
    /// called eve if your thread panics). However, you can use this function to
    ///
    /// 1. Make periodical backups. Use an external timer implementation for this.
    ///
    /// 2. Handle possible IO errors in sending. The `drop` implementation will
    /// ignore (but log) any io errors, which may lead to data loss in an
    /// unreliable filesystem. It was implmemented this way because no errors
    /// are allowed to propagate on drop and panicking will abort the program if
    /// drop is called during a panic.
    #[deprecated(
        since = "0.5.0",
        note = "the sender state is now always inferred. There is no need to save anything"
    )]
    pub fn save(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Just writes to the internal buffer, but doesn't flush it.
    fn write(&mut self, data: &[u8]) -> io::Result<u64> {
        // Get length of the data and write the header:
        let len = data.as_ref().len();
        assert!(len < std::u64::MAX as usize);
        let header = Header::new(len as u32).encode();

        // Write stuff to the file:
        self.file.write_all(&header)?;
        self.file.write_all(data.as_ref())?;

        Ok(4 + len as u64)
    }

    /// Tests whether the queue is past the end of the current segment.
    fn is_past_end(&self) -> bool {
        self.state.position > self.max_segment_size.get()
    }

    /// Caps off a segment by writing an EOF header and then moves segment.
    /// This function returns `Ok(true)` if it has created a new segment or
    /// `Ok(false)` if it has not (because the queue was too big).
    #[must_use = "you need to always check if a segment was created or not!"]
    fn try_cap_off_and_move(&mut self) -> io::Result<bool> {
        if let Some(max_queue_size) = self.max_queue_size {
            if get_dir_size(&self.base)? >= max_queue_size.get() {
                return Ok(false);
            }
        }

        // Write EOF header:
        self.file.write(&HEADER_EOF)?;
        self.file.flush()?;

        // Preserves the already allocated buffer:
        *self.file.get_mut() = OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_filename(&self.base, self.state.advance_segment()))?;

        Ok(true)
    }

    fn maybe_cap_off_and_move<T>(&mut self, item: T) -> Result<T, TrySendError<T>> {
        // See if you are past the end of the file
        if self.is_past_end() {
            // If so, create a new file, if you are able to:
            if !self.try_cap_off_and_move()? {
                return Err(TrySendError::QueueFull {
                    item,
                    queue_name: format!("{:?}", self.base),
                });
            }
        }

        Ok(item)
    }

    /// Lazy inits the future that completes every time a file is deleted.
    fn deletion_stream(&mut self) -> &mut DeletionEvent {
        if self.deletion_stream.is_none() {
            let deletion_stream = DeletionEvent::new(&self.base);
            self.deletion_stream = Some(deletion_stream);
        }

        self.deletion_stream.as_mut().unwrap() // because if was not Some, now it is.
    }

    /// Tries to sends some data into the queue. If the queue is too big to
    /// insert (as set in `max_queue_size`), this returns
    /// [`TrySendError::QueueFull`]. One send is always atomic.
    ///
    /// # Errors
    ///
    /// This function returns any underlying errors encountered while writing or
    /// flushing the queue. Also, it returns [`TrySendError::QueueFull`] if the
    /// queue is too big.
    pub fn try_send<D: AsRef<[u8]>>(&mut self, data: D) -> Result<(), TrySendError<D>> {
        let data = self.maybe_cap_off_and_move(data)?;

        // Write to the queue and flush:
        let written = self.write(data.as_ref())?;
        self.file.flush()?; // guarantees atomic operation. See `new`.
        self.state.advance_position(written);

        Ok(())
    }

    /// Sends some data into the queue. One send is always atomic. This function is
    /// `async` because the queue might be full and so we need to `.await` the
    /// receiver to consume enough segments to clear the queue.
    ///
    /// # Errors
    ///
    /// This function returns any underlying errors encountered while writing or
    /// flushing the queue.
    ///
    pub async fn send<D: AsRef<[u8]>>(&mut self, mut data: D) -> io::Result<()> {
        loop {
            match self.try_send(data) {
                Ok(()) => break Ok(()),
                Err(TrySendError::Io(err)) => break Err(err),
                Err(TrySendError::QueueFull { item, .. }) => {
                    data = item; // the "unmove"!
                    self.deletion_stream().await // prevents spinlock
                }
            }
        }
    }

    /// Tries to send all the contents of an iterable into the queue. If the
    /// queue is too big to insert (as set in `max_queue_size`), this returns
    /// [`TrySendError::QueueFull`]. All is buffered to be sent atomically, in
    /// one flush operation. Since this operation is atomic, it does not create
    /// new segments during the iteration. Be mindful of that when using this
    /// method for large writes.
    ///
    /// # Errors
    ///
    /// This function returns any underlying errors encountered while writing or
    /// flushing the queue. Also, it returns [`TrySendError::QueueFull`] if the
    /// queue is too big.
    pub fn try_send_batch<I>(&mut self, it: I) -> Result<(), TrySendError<I>>
    where
        I: IntoIterator,
        I::Item: AsRef<[u8]>,
    {
        let it = self.maybe_cap_off_and_move(it)?;

        let mut written = 0;
        // Drain iterator into the buffer.
        for item in it {
            written += self.write(item.as_ref())?;
        }

        self.file.flush()?; // guarantees atomic operation. See `new`.
        self.state.advance_position(written);

        Ok(())
    }

    pub async fn send_batch<I>(&mut self, mut it: I) -> io::Result<()>
    where
        I: IntoIterator,
        I::Item: AsRef<[u8]>,
    {
        loop {
            match self.try_send_batch(it) {
                Ok(()) => break Ok(()),
                Err(TrySendError::Io(err)) => break Err(err),
                Err(TrySendError::QueueFull { item, .. }) => {
                    it = item; // the "unmove"!
                    self.deletion_stream().await // prevents spinlock
                }
            }
        }
    }
}
