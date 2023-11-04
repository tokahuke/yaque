use futures::future;
use futures::FutureExt;
use std::collections::VecDeque;
use std::fs::*;
use std::future::Future;
use std::io::{self};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::error::TryRecvError;
use crate::header::Header;
use crate::state::QueueState;
use crate::state::QueueStatePersistence;
use crate::sync::{FileGuard, TailFollower};
use crate::version::check_queue_version;

use super::{segment_filename, HEADER_EOF};

/// The name of the receiver lock in the queue folder.
pub(crate) fn recv_lock_filename<P: AsRef<Path>>(base: P) -> PathBuf {
    base.as_ref().join("recv.lock")
}

/// Tries to acquire the receiver lock for a queue.
pub(crate) fn try_acquire_recv_lock<P: AsRef<Path>>(base: P) -> io::Result<FileGuard> {
    FileGuard::try_lock(recv_lock_filename(base.as_ref()))?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            format!(
                "queue `{}` receiver side already in use",
                base.as_ref().to_string_lossy()
            ),
        )
    })
}

/// Acquire the receiver lock for a queue, awaiting if locked.
pub(crate) async fn acquire_recv_lock<P: AsRef<Path>>(base: P) -> io::Result<FileGuard> {
    FileGuard::lock(recv_lock_filename(base.as_ref())).await
}

/// A builder for the receiver side of the queue. Use this if you want to have
/// fine-grained control over the configuration of the queue. Most defaults
/// should be ok of most applications.
pub struct ReceiverBuilder {
    save_every_nth: Option<usize>,
    save_every: Option<Duration>,
}

impl Default for ReceiverBuilder {
    fn default() -> ReceiverBuilder {
        ReceiverBuilder {
            save_every_nth: Some(250),
            save_every: Some(Duration::from_millis(350)),
        }
    }
}

impl ReceiverBuilder {
    pub fn new() -> ReceiverBuilder {
        ReceiverBuilder::default()
    }

    /// Sets the receiver to save its state to the disk at every n-th element.
    /// Set it to `None` to disable this policy.
    ///
    /// Default value: every 250 elements.
    pub fn save_every_nth(mut self, nth: Option<usize>) -> ReceiverBuilder {
        self.save_every_nth = nth;
        self
    }

    /// Sets the receiver to save ir state to the disk at every given interval
    /// of time. Set it to `None` to disable this policy.
    ///
    /// Default value: every 350 milliseconds.
    ///
    /// # Note:
    ///
    /// This policy is enforced _synchronously_. This means that there is no
    /// asynchronous behavior involved (i.e. timers). This condition will only
    /// be checked when a new element is pushed to the queue.
    pub fn save_every(mut self, duration: Option<Duration>) -> ReceiverBuilder {
        self.save_every = duration;
        self
    }

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
    pub fn open<P: AsRef<Path>>(self, base: P) -> io::Result<Receiver> {
        // Guarantee that the queue exists:
        create_dir_all(base.as_ref())?;

        log::trace!("created queue directory");

        // Versioning stuff (this should be lightning-fast. Therefore, shameless block):
        check_queue_version(base.as_ref())?;

        // Acquire guard and state:
        let file_guard = try_acquire_recv_lock(base.as_ref())?;
        let mut persistence = QueueStatePersistence::new();
        let state = persistence.open(base.as_ref())?;

        log::trace!("receiver lock acquired. Receiver state now is {:?}", state);

        // Put the needle on the groove (oh! the 70's):
        let mut tail_follower =
            TailFollower::open(&segment_filename(base.as_ref(), state.segment))?;
        tail_follower.seek(io::SeekFrom::Start(state.position))?;

        log::trace!("last segment opened fo reading");

        Ok(Receiver {
            _file_guard: file_guard,
            tail_follower,
            maybe_header: None,
            state,
            initial_state: state,
            base: PathBuf::from(base.as_ref()),
            persistence,
            read_and_unused: VecDeque::new(),
            save_every: self.save_every,
            save_every_nth: self.save_every_nth,
            n_reads: 0,
            last_saved_at: Instant::now(),
        })
    }
}

/// The receiver part of the queue. This part is asynchronous and therefore
/// needs an executor that will the poll the futures to completion.
pub struct Receiver {
    /// The path to the folder holding the queue.
    base: PathBuf,
    /// The acquired receiver lock file for this queue.
    _file_guard: FileGuard,
    /// The current segment being tailed.
    tail_follower: TailFollower,
    /// The last header read from the queue.
    maybe_header: Option<[u8; 4]>,
    /// The current queue state.
    state: QueueState,
    /// The queue state as it was in the beginning of the current transaction.
    initial_state: QueueState,
    /// The queue state saver/loader.
    persistence: QueueStatePersistence,
    /// Use this queue to buffer elements and provide "atomicity in an
    /// asynchronous context". We need to backup the state of the queue before
    /// the read so as to restore it as the "initial state" (the _actual_ state
    /// of the queue) at the end of a transaction. Otherwise, data loss would
    /// occur.
    read_and_unused: VecDeque<Vec<u8>>,
    /// Save the queue every n operations
    save_every_nth: Option<usize>,
    /// Save the queue every interval of time. This will be enforced
    /// _synchronously_; no timers involved.
    save_every: Option<Duration>,
    /// Number of operations done in this `Receiver`
    n_reads: usize,
    /// Last time the queue was saved:
    last_saved_at: Instant,
}

impl Receiver {
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
    pub fn open<P: AsRef<Path>>(base: P) -> io::Result<Receiver> {
        ReceiverBuilder::default().open(base)
    }

    /// Starts a transaction in the queue.
    fn begin(&mut self) {
        log::debug!("begin transaction in {:?} at {:?}", self.base, self.state);
    }

    /// Puts the queue in another position in another segment. This forcibly
    /// discards the old tail follower and fetches a fresh new one, so be
    /// careful.
    fn go_to(&mut self, state: QueueState) -> io::Result<()> {
        let different_segment = self.state.segment != state.segment;

        log::debug!("going from {:?} to {:?}", self.state, state);
        self.state = state;

        if different_segment {
            log::debug!("opening segment {}", self.state.segment);
            self.tail_follower =
                TailFollower::open(&segment_filename(&self.base, self.state.segment))?;
        }

        self.tail_follower
            .seek(io::SeekFrom::Start(state.position))?;

        Ok(())
    }

    /// Deletes old segments from a given point in time and makes the current
    /// state the initial state.
    fn end(&mut self) -> io::Result<()> {
        assert!(
            self.state.segment >= self.initial_state.segment,
            "advanced to a past position. Initial was {:?}; current is {:?}",
            self.initial_state,
            self.state
        );

        for segment_id in self.initial_state.segment..self.state.segment {
            log::debug!("removing segment {} from {:?}", segment_id, self.base);
            remove_file(segment_filename(&self.base, segment_id))?;
        }

        log::debug!(
            "end transaction in {:?} at {:?} (from {:?})",
            self.base,
            self.state,
            self.initial_state
        );

        // // Reason: think you read with timeout 7 items, but wanted 10. Then, you read with timeout
        // // 3 items, leaving 4 read and unused. Therefore, the Receiver has read 4 elements ahead,
        // // which you have not seen. Therefore, initial_state cannot be state in the case, since you
        // // would lose 4 elements. It has to be the position of the _next_ element in the read and
        // // unused queue.
        // self.initial_state = if let Some((_data, state)) = self.read_and_unused.front() {
        //     *state // the state that was before the next element was read.
        // } else {
        //     self.state
        // };

        // Everything you read has to be used. Otherwise, setting initial state to state loses data.
        assert!(
            self.read_and_unused.is_empty(),
            "There were read and unused items at the end of transaction. Read and unused queue: {:?}",
            self.read_and_unused
        );
        self.initial_state = self.state;

        // Alternatively... if you make read and unused VecDeque<(Vec<u8>, QueueState)> to backup the
        // state, you can do the following (deprecated code):

        // // Reason: think you read with timeout 7 items, but wanted 10. Then, you read with timeout
        // // 3 items, leaving 4 read and unused. Therefore, the Receiver has read 4 elements ahead,
        // // which you have not seen. Therefore, initial_state cannot be state in the case, since you
        // // would lose 4 elements. It has to be the position of the _next_ element in the read and
        // // unused queue.
        // self.initial_state = if let Some((_data, state)) = self.read_and_unused.front() {
        //     *state // the state that was before the next element was read.
        // } else {
        //     self.state
        // };

        // Finally save if it is time to save:
        self.maybe_save()?;

        Ok(())
    }

    /// Reads the header. This operation is atomic.
    async fn read_header(&mut self) -> io::Result<Header> {
        // If the header was already read (by an incomplete operation), use it!
        if let Some(header) = self.maybe_header {
            return Ok(Header::decode(header));
        }

        // Read header:
        let mut header = [0; 4];
        self.tail_follower.read_exact(&mut header).await?;

        // If the header is EOF, advance segment:
        if header == HEADER_EOF {
            log::trace!("got EOF header. Advancing...");
            let mut new_state = self.state.clone();
            new_state.advance_segment();
            self.go_to(new_state)?; // forces to open new segment.

            // Re-read the header:
            log::trace!("re-reading new header from new file");
            self.tail_follower.read_exact(&mut header).await?;
        }

        // Now, you set the header!
        self.maybe_header = Some(header.clone());
        let decoded = Header::decode(header);
        self.state.advance_position(4);

        log::trace!("got header {:?} (read {} bytes)", header, decoded.len());

        Ok(decoded)
    }

    /// Reads one element from the queue, inevitably advancing the file reader.
    /// Instead of returning the element, this function puts it in the "read and
    /// unused" queue to be used later. This enables us to construct "atomic in
    /// async context" guarantees for the higher level functions. The idea is to
    /// _drain the queue_ only after the last `.await` in the block.
    ///
    /// This operation is also itself atomic. If the returned future is not
    /// polled to completion, as, e.g., when calling `select`, the operation
    /// will count as not done.
    async fn read_one(&mut self) -> io::Result<()> {
        // Get the length:
        let header = self.read_header().await?;

        // With the length, read the data:
        let mut data = vec![0; header.len() as usize];
        if header.len() > 0 {
            // If data size is 0, no need to read any data at all.
            self.tail_follower
                .read_exact(&mut data)
                .await
                .expect("poisoned queue");
        }

        self.state.advance_position(data.len() as u64);

        // We are done! Unset header:
        self.maybe_header = None;

        // Ready to be used:
        self.read_and_unused.push_back(data);

        // Bookkeeping:
        self.n_reads += 1;

        Ok(())
    }

    /// Reads one element from the queue until a future elapses. If the future
    /// elapses first, then `OK(false)` is returned and no element is put in
    /// the "read and unused" internal queue. Otherwise, `Ok(true)` is returned
    /// and exactly one element is put in the "read and unused" queue.
    ///
    /// This operation is atomic. If the returned future is not polled to
    /// completion, as, e.g., when calling `select`, the operation will be
    /// undone.
    async fn read_one_timeout<F>(&mut self, timeout: F) -> io::Result<bool>
    where
        F: Future<Output = ()> + Unpin,
    {
        match future::select(Box::pin(self.read_one()), timeout).await {
            future::Either::Left((read_one, _)) => read_one.map(|_| true),
            future::Either::Right((_, _)) => Ok(false),
        }
    }

    /// Drains `n` elements from the "read and unused" queue into a vector. This
    /// operation is "atomic in an async context", since it is not `async`. For a
    /// function to enjoy the same guarantee, this function must only be called
    /// after the last `.await` in the caller's control flow.
    fn drain(&mut self, n: usize) -> Vec<Vec<u8>> {
        let mut data = Vec::with_capacity(n);

        // (careful! need to check if read something to avoid an erroneous POP
        // from the queue)
        if n > 0 {
            while let Some(element) = self.read_and_unused.pop_front() {
                data.push(element);

                if data.len() == n {
                    break;
                }
            }
        }

        data
    }

    /// Saves the receiver queue state. You do not need to use method in most
    /// circumstances, since it is automatically done on drop (yes, it will be
    /// called eve if your thread panics). However, you can use this function to
    ///
    /// 1. Make periodical backups. Use an external timer implementation for this.
    ///
    /// 2. Handle possible IO errors in logging the state of the queue to the disk
    /// after commit. The `drop` implementation will ignore (but log) any io
    /// errors, which may lead to data loss in an unreliable filesystem. It was
    /// implemented this way because no errors are allowed to propagate on drop
    /// and panicking will abort the program if drop is called during a panic.
    pub fn save(&mut self) -> io::Result<()> {
        self.persistence.save(&self.initial_state) // this avoids saving an in-flight
    }

    fn maybe_save(&mut self) -> io::Result<()> {
        if let Some(save_every_nth) = self.save_every_nth {
            if self.n_reads % save_every_nth == 0 {
                self.save()?;
            }
        } else if let Some(save_every) = self.save_every {
            if self.last_saved_at.elapsed() >= save_every {
                self.save()?;
            }
        }

        Ok(())
    }

    /// Retrieves an element from the queue. The returned value is a
    /// guard that will only commit state changes to the queue when dropped.
    ///
    /// This operation is atomic. If the returned future is not polled to
    /// completion, as, e.g., when calling `select`, the operation will be
    /// undone.
    ///
    /// # Panics
    ///
    /// This function will panic if it has to start reading a new segment and
    /// it is not able to set up the notification handler to watch for file
    /// changes.
    pub async fn recv(&mut self) -> io::Result<RecvGuard<'_, Vec<u8>>> {
        self.begin();

        let data = if let Some(data) = self.read_and_unused.pop_front() {
            data
        } else {
            self.read_one().await?;
            self.read_and_unused
                .pop_front()
                .expect("guaranteed to yield an element")
        };

        Ok(RecvGuard {
            receiver: self,
            item: Some(data),
            was_finished: false,
        })
    }

    /// Tries to retrieve an element from the queue. The returned value is a
    /// guard that will only commit state changes to the queue when dropped.
    ///
    /// # Panics
    ///
    /// This function will panic if it has to start reading a new segment and
    /// it is not able to set up the notification handler to watch for file
    /// changes.
    pub fn try_recv(&mut self) -> Result<RecvGuard<'_, Vec<u8>>, TryRecvError> {
        TryRecvError::result_from_option(self.recv().now_or_never())
    }

    /// Retrieves an element from the queue until a given future
    /// finishes, whichever comes first. If an element arrives first, the
    /// returned value is a guard that will only commit state changes to the
    /// queue when dropped. Otherwise, `Ok(None)` is returned.
    ///
    /// This operation is atomic. If the returned future is not polled to
    /// completion, as, e.g., when calling `select`, the operation will be
    /// undone.
    ///
    /// # Panics
    ///
    /// This function will panic if it has to start reading a new segment and
    /// it is not able to set up the notification handler to watch for file
    /// changes.
    pub async fn recv_timeout<F>(
        &mut self,
        timeout: F,
    ) -> io::Result<Option<RecvGuard<'_, Vec<u8>>>>
    where
        F: Future<Output = ()> + Unpin,
    {
        self.begin();

        let data = if let Some(data) = self.read_and_unused.pop_front() {
            data
        } else {
            if self.read_one_timeout(timeout).await? {
                self.read_and_unused
                    .pop_front()
                    .expect("guaranteed to yield an element")
            } else {
                return Ok(None);
            }
        };

        Ok(Some(RecvGuard {
            receiver: self,
            item: Some(data),
            was_finished: false,
        }))
    }

    /// Removes exactly 'n' elements from the queue. If there aren't
    /// enough elements, it will wait until there are. Returns a guard
    /// that will only commit state changes to the queue when dropped.
    ///
    /// # Note
    ///
    /// This operation is atomic in an asynchronous context. This means that you
    /// will not lose the elements if you do not await this function to
    /// completion.
    ///
    /// # Panics
    ///
    /// This function will panic if it has to start reading a new segment and
    /// it is not able to set up the notification handler to watch for file
    /// changes.
    pub async fn recv_batch(&mut self, n: usize) -> io::Result<RecvGuard<'_, Vec<Vec<u8>>>> {
        self.begin();

        // First, fetch what is missing from the disk:
        if n > self.read_and_unused.len() {
            for _ in 0..(n - self.read_and_unused.len()) {
                self.read_one().await?;
            }
        }

        // And now, drain!
        let data = self.drain(n);

        Ok(RecvGuard {
            receiver: self,
            item: Some(data),
            was_finished: false,
        })
    }

    /// Remove up to 'n' elements from the queue.
    ///
    /// If the queue has data in it, this will immediately remove the
    /// first 'n' elements.  If the queue is empty, it will await
    /// until there is some data to return, then return the first
    /// element.
    ///
    /// The returned value is a guard that will only commit state
    /// changes to the queue when dropped.
    ///
    /// # Panics
    ///
    /// This function will panic if it has to start reading a new segment and
    /// it is not able to set up the notification handler to watch for file
    /// changes.
    pub async fn recv_batch_up_to(&mut self, n: usize) -> io::Result<RecvGuard<'_, Vec<Vec<u8>>>> {
        self.begin();

        // First, read everything we currently have available into read_and_unused
        while self.read_and_unused.len() < n {
            if self.read_one().now_or_never().is_none() {
                break;
            }
        }

        // Then, if it's empty and we're looking for /something/, block until we have at least one thing
        if n > 0 {
            if self.read_and_unused.is_empty() {
                self.read_one().await?;
            }
        }

        // And now, drain!
        let data = self.drain(n);

        Ok(RecvGuard {
            receiver: self,
            item: Some(data),
            was_finished: false,
        })
    }

    /// Tries to remove a number of elements from the queue. The returned value
    /// is a guard that will only commit state changes to the queue when
    /// dropped.
    ///
    /// # Panics
    ///
    /// This function will panic if it has to start reading a new segment and
    /// it is not able to set up the notification handler to watch for file
    /// changes.
    pub fn try_recv_batch(
        &mut self,
        n: usize,
    ) -> Result<RecvGuard<'_, Vec<Vec<u8>>>, TryRecvError> {
        TryRecvError::result_from_option(self.recv_batch(n).now_or_never())
    }

    /// Remove up to 'n' elements from the queue
    ///
    /// If the queue has data in it, this will immediately remove the
    /// first 'n' elements.  If the queue is empty, returns
    /// Err(TryRecvError::QueueEmpty).
    ///
    /// The returned value is a guard that will only commit state
    /// changes to the queue when dropped.
    ///
    /// # Panics
    ///
    /// This function will panic if it has to start reading a new segment and
    /// it is not able to set up the notification handler to watch for file
    /// changes.
    pub fn try_recv_batch_up_to(
        &mut self,
        n: usize,
    ) -> Result<RecvGuard<'_, Vec<Vec<u8>>>, TryRecvError> {
        TryRecvError::result_from_option(self.recv_batch_up_to(n).now_or_never())
    }

    /// Tries to remove a number of elements from the queue until a given future
    ///  finished. The values taken from the queue will be the values that were
    /// available during the whole execution of the future and thus less than `n`
    /// elements might be returned. The returned items are wrapped in a guard
    /// that will only commit state changes to the queue when dropped.
    ///
    /// # Note
    ///
    /// This operation is atomic in an asynchronous context. This means that you
    /// will not lose the elements if you do not await this function to
    /// completion.
    ///
    /// # Panics
    ///
    /// This function will panic if it has to start reading a new segment and
    /// it is not able to set up the notification handler to watch for file
    /// changes.
    pub async fn recv_batch_timeout<F>(
        &mut self,
        n: usize,
        mut timeout: F,
    ) -> io::Result<RecvGuard<'_, Vec<Vec<u8>>>>
    where
        F: Future<Output = ()> + Unpin,
    {
        self.begin();
        let mut n_read = 0;

        // First, fetch what is missing from the disk:
        if n > self.read_and_unused.len() {
            for _ in 0..(n - self.read_and_unused.len()) {
                if !self.read_one_timeout(&mut timeout).await? {
                    break;
                } else {
                    n_read += 1;
                }
            }
        }

        // And now, drain!
        let data = self.drain(n_read);

        Ok(RecvGuard {
            receiver: self,
            item: Some(data),
            was_finished: false,
        })
    }

    /// Takes a number of elements from the queue until a certain asynchronous
    /// condition is met. Use this function if you want to have fine-grained
    /// control over the contents of the receive guard.
    ///
    /// Note that the predicate function will receive a `None` as the first
    /// element. This allows you to return early and leave the queue intact.
    /// The returned value is a guard that will only commit state changes to
    /// the queue when dropped.
    ///
    /// # Note
    ///
    /// This operation is atomic in an asynchronous context. This means that you
    /// will not lose the elements if you do not await this function to
    /// completion.
    ///
    /// # Example
    ///
    /// Receive until an empty element is received:
    /// ```ignore
    /// let recv_guard = receiver.recv_until(|element| async { element.is_empty() }).await;
    /// ```
    ///
    /// # Panics
    ///
    /// This function will panic if it has to start reading a new segment and
    /// it is not able to set up the notification handler to watch for file
    /// changes.
    pub async fn recv_until<P, Fut>(
        &mut self,
        mut predicate: P,
    ) -> io::Result<RecvGuard<'_, Vec<Vec<u8>>>>
    where
        P: FnMut(Option<&[u8]>) -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        self.begin();
        let mut n_read = 0;

        // Prepare:
        predicate(None).await;

        // Poor man's do-while (aka. until)
        // Strategy: fill `read_and_unused` to the brim and then drain at the end.
        loop {
            // Need to fetch from disk?
            if n_read == self.read_and_unused.len() {
                self.read_one().await?;
            }

            let item_ref = &self.read_and_unused[n_read];

            if !predicate(Some(item_ref)).await {
                n_read += 1;
            } else {
                break;
            }
        }

        // And now, drain!
        let data = self.drain(n_read);

        Ok(RecvGuard {
            receiver: self,
            item: Some(data),
            was_finished: false,
        })
    }

    /// Tries to take a number of elements from the queue until a certain
    /// *synchronous* condition is met. Use this function if you want to have
    /// fine-grained control over the contents of the receive guard.
    ///
    /// Note that the predicate function will receive a `None` as the first
    /// element. This allows you to return early and leave the queue intact.
    /// The returned value is a guard that will only commit state changes to
    /// the queue when dropped.
    ///
    /// # Example
    ///
    /// Try to receive until an empty element is received:
    /// ```ignore
    /// let recv_guard = receiver.try_recv_until(|element| element.is_empty());
    /// ```
    ///
    /// # Panics
    ///
    /// This function will panic if it has to start reading a new segment and
    /// it is not able to set up the notification handler to watch for file
    /// changes.
    pub fn try_recv_until<P, Fut>(
        &mut self,
        mut predicate: P,
    ) -> Result<RecvGuard<'_, Vec<Vec<u8>>>, TryRecvError>
    where
        P: FnMut(Option<&[u8]>) -> bool,
    {
        TryRecvError::result_from_option(
            self.recv_until(move |el| {
                let outcome = predicate(el);
                async move { outcome }
            })
            .now_or_never(),
        )
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        if let Err(err) = self.save() {
            log::error!(
                "(probably) could not save queue state during `Drop`: {}",
                err
            );
        }
    }
}

/// A guard that will only log changes on the queue state when dropped.
///
/// If it is dropped without a call to `RecvGuard::commit`, changes will be
/// rolled back in a "best effort" policy: if any IO error is encountered
/// during rollback, the state will be committed. If you *can* do something
/// with the IO error, you may use `RecvGuard::rollback` explicitly to catch
/// the error.  
///
/// This struct implements `Deref` and `DerefMut`. If you really, really want
/// ownership, there is `RecvGuard::into_inner`, but be careful, because you
/// lose your chance to rollback if anything unexpected occurs.
pub struct RecvGuard<'a, T> {
    receiver: &'a mut Receiver,
    item: Option<T>,
    was_finished: bool,
}

impl<'a, T> Drop for RecvGuard<'a, T> {
    fn drop(&mut self) {
        if !self.was_finished {
            if let Err(err) = self.rollback_mut() {
                log::error!("unable to rollback on drop: {}", err);
            }
        }
    }
}

impl<'a, T> Deref for RecvGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.item.as_ref().expect("unreachable")
    }
}

impl<'a, T> DerefMut for RecvGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.item.as_mut().expect("unreachable")
    }
}

impl<'a, T> RecvGuard<'a, T> {
    /// Commits the transaction and returns the underlying value. If you
    /// accidentally lose this value from now on, it's your own fault!
    pub fn try_into_inner(mut self) -> io::Result<T> {
        let item = self.item.take().expect("unreachable");
        self.commit()?;

        Ok(item)
    }

    /// Commits the changes to the queue, consuming this `RecvGuard`.
    pub fn commit(mut self) -> io::Result<()> {
        self.receiver.end()?;
        self.was_finished = true;

        Ok(())
    }

    /// Same as rollback, but doesn't consume the guard. This is for internal use only.
    fn rollback_mut(&mut self) -> io::Result<()> {
        self.receiver.go_to(self.receiver.initial_state)?;
        self.receiver.end()?;
        self.was_finished = true;

        Ok(())
    }

    /// Rolls the reader back to the previous point, negating the changes made
    /// on the queue. This is also done on drop. However, on drop, the possible
    /// IO error is ignored (but logged as an error) because we cannot have
    /// errors inside drops. Use this if you want to control errors at rollback.
    ///
    /// # Errors
    ///
    /// If there is some error while moving the reader back, this error will be
    /// return.
    pub fn rollback(mut self) -> io::Result<()> {
        self.rollback_mut()
    }
}
