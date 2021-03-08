use futures::future;
use std::collections::VecDeque;
use std::fs::*;
use std::future::Future;
use std::io::{self};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use crate::version::{check_queue_version};
use crate::header::Header;
use crate::state::QueueState;
use crate::state::QueueStatePersistence;
use crate::sync::{FileGuard, TailFollower};

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

/// The receiver part of the queue. This part is asynchronous and therefore
/// needs an executor that will the poll the futures to completion.
pub struct Receiver {
    _file_guard: FileGuard,
    tail_follower: TailFollower,
    maybe_header: Option<[u8; 4]>,
    state: QueueState,
    base: PathBuf,
    persistence: QueueStatePersistence,
    /// Use this queue to buffer elements and provide "atomicity in an
    /// asynchronous context".
    read_and_unused: VecDeque<Vec<u8>>,
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
        let mut tail_follower = TailFollower::open(segment_filename(base.as_ref(), state.segment))?;
        tail_follower.seek(io::SeekFrom::Start(state.position))?;

        log::trace!("last segment opened fo reading");

        Ok(Receiver {
            _file_guard: file_guard,
            tail_follower,
            maybe_header: None,
            state,
            base: PathBuf::from(base.as_ref()),
            persistence,
            read_and_unused: VecDeque::new(),
        })
    }

    /// Maybe advance the segment of this receiver.
    fn advance(&mut self) -> io::Result<()> {
        log::trace!(
            "advancing segment from {} to {}",
            self.state.segment,
            self.state.segment + 1
        );

        // Advance segment and checkpoint!
        self.state.advance_segment();
        if let Err(err) = self.persistence.save(&self.state) {
            log::error!("failed to save receiver: {}", err);
            self.state.retreat_segment();
            return Err(err);
        }

        // Start listening to new segments:
        self.tail_follower = TailFollower::open(segment_filename(&self.base, self.state.segment))?;

        log::trace!("acquired new tail follower");

        // Remove old file (needs to be done *after* opening new file, as there must _always_ be a
        // segment for the sender to infer its position correcty)
        remove_file(segment_filename(&self.base, self.state.segment - 1))?;

        log::trace!("removed old segment file");

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
            self.advance()?;

            // Re-read the header:
            log::trace!("re-reading new header from new file");
            self.tail_follower.read_exact(&mut header).await?;
        }

        // Now, you set the header!
        self.maybe_header = Some(header.clone());
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
    async fn read_one(&mut self) -> io::Result<()> {
        // Get the length:
        let header = self.read_header().await?;

        // With the length, read the data:
        let mut data = vec![0; header.len() as usize];
        self.tail_follower
            .read_exact(&mut data)
            .await
            .expect("poisoned queue");

        // We are done! Unset header:
        self.maybe_header = None;

        // Ready to be used:
        self.read_and_unused.push_back(data);

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

        // (careful! need to check if read something to avoid an eroneous POP
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
        self.persistence.save(&self.state)
    }

    /// Tries to retrieve an element from the queue. The returned value is a
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
            len: 4 + data.len(),
            item: Some(data),
            override_drop: false,
        })
    }

    /// Tries to retrieve an element from the queue until a given future
    /// finishes. If an element arrives first, he returned value is a guard
    /// that will only commit state changes to the queue when dropped.
    /// Otherwise, `Ok(None)` is returned.
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
            len: 4 + data.len(),
            item: Some(data),
            override_drop: false,
        }))
    }

    /// Tries to remove a number of elements from the queue. The returned value
    /// is a guard that will only commit state changes to the queue when dropped.
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
            len: data.iter().map(|item| 4 + item.len()).sum(),
            item: Some(data),
            override_drop: false,
        })
    }

    /// Tries to remove a number of elements from the queue until a given future
    ///  finished. The values taken from the queue will be the values that were
    /// available durng the whole execution of the future and thus less than `n`
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
            len: data.iter().map(|item| 4 + item.len()).sum(),
            item: Some(data),
            override_drop: false,
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
    /// let recv_guard = receiver.recv_until(|element| async { element.is_empty() });
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
        let mut n_read = 0;

        // Prepare:
        predicate(None).await;

        // Poor man's do-while (aka. until)
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
            len: data.iter().map(|item| 4 + item.len()).sum(),
            item: Some(data),
            override_drop: false,
        })
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        if let Err(err) = self.persistence.save(&self.state) {
            log::error!("could not release receiver lock: {}", err);
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
    len: usize,
    item: Option<T>,
    override_drop: bool,
}

impl<'a, T> Drop for RecvGuard<'a, T> {
    fn drop(&mut self) {
        if self.override_drop {
            // do nothing!
        } else {
            if let Err(err) = self
                .receiver
                .tail_follower
                .seek(io::SeekFrom::Current(-(self.len as i64)))
            {
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
    pub fn into_inner(mut self) -> T {
        let item = self.item.take().expect("unreachable");
        self.commit();

        item
    }

    /// Commits the changes to the queue, consuming this `RecvGuard`.
    pub fn commit(mut self) {
        self.override_drop = true;
        self.receiver.state.position += self.len as u64;
        drop(self);
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
        self.override_drop = true;

        // Do it manually.
        self.receiver
            .tail_follower
            .seek(io::SeekFrom::Current(-(self.len as i64)))
    }
}
