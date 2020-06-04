//! # Yaque: Yet Another QUEue
//!
//! Yaque is yet another disk-backed persistent queue for Rust. It implements
//! an SPSC channel using you OS' filesystem. Its main advantages over a simple
//! `VecDeque<T>` are that
//! * You are not constrained by your RAM size, just by your disk size. This
//! means you can store gigabytes of data without getting OOM killed.
//! * Your data is safe even if you program panics. All the queue state is
//! written to the disk when the queue is dropped.
//! * Your data can *persist*, that is, can exisit thrhough multiple executions
//! of your program. Think of it as a very rudimentary kind of database.
//! * You can pass data between two processes.
//!
//! Yaque is _assynchronous_ and built directly on top of `mio` and `notify`.
//! It is therefore completely agnostic to the runtime you are using for you
//! application. It will work smoothly with `tokio`, with `async-std` or any
//! other executor of your choice.
//!
//! Yaque is also agnostic to the persistence method _of the queue state_. Even
//! though data will be written to filesystem, the state of the queue, that is,
//! the positions of the reader and of the sender, may be saved elsewhere. Examples
//! include a database, S3 or any other external persistence service. For your
//! conveinence, a local `FilePersistence` is provided that will record the
//! queue state to the queue folder itself.  
//!
//! ## To do's:
//!
//! * Clear queue.
//! * Channel convenience.
//! * Send batch.

mod state;
mod sync;

use std::fs::*;
use std::io::{self, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use state::{FileGuard, QueueState};
use sync::TailFollower;

pub use state::{FilePersistence, Persist};

/// The name of segment file in the queue folder.
fn segment_filename<P: AsRef<Path>>(base: P, segment: u64) -> PathBuf {
    base.as_ref().join(format!("{}.q", segment))
}

/// The name of the receiver lock in the queue folder.
fn recv_lock_filename<P: AsRef<Path>>(base: P) -> PathBuf {
    base.as_ref().join("recv.lock")
}

/// Tries to acquire the receiver lock for a queue.
fn acquire_recv_lock<P: AsRef<Path>>(base: P) -> io::Result<FileGuard> {
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

/// The name of the sender lock in the queue folder.
fn send_lock_filename<P: AsRef<Path>>(base: P) -> PathBuf {
    base.as_ref().join("send.lock")
}

/// Tries to acquire the sender lock for a queue.
fn acquire_send_lock<P: AsRef<Path>>(base: P) -> io::Result<FileGuard> {
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

/// The sender part of the queue.
pub struct Sender<Ps: Persist> {
    _file_guard: FileGuard,
    file: io::BufWriter<File>,
    state: QueueState,
    base: PathBuf,
    persist: Ps,
}

impl<Ps: Persist> Sender<Ps> {
    /// Opens a queue for sending.
    pub fn open<P: AsRef<Path>>(base: P, mut persist: Ps) -> io::Result<Sender<Ps>> {
        // Guarantee that the queue exists:
        create_dir_all(base.as_ref())?;

        // Acquire lock and state:
        let file_guard = acquire_send_lock(base.as_ref())?;
        let state = persist.open_send(base.as_ref())?;

        // See the docs on OpenOptions::append for why the BufWriter here.
        let file = io::BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(segment_filename(base.as_ref(), state.segment))?,
        );

        Ok(Sender {
            _file_guard: file_guard,
            file,
            state,
            base: PathBuf::from(base.as_ref()),
            persist,
        })
    }

    /// Sends some data into the queue. One send is always atomic.
    pub fn send(&mut self, data: &[u8]) -> io::Result<()> {
        // Get length of the data and write the header:
        let len = data.len();
        assert!(len < std::u32::MAX as usize);
        let header = (len as u32).to_be_bytes();

        // Write stuff to the file:
        self.file.write_all(&header)?;
        self.file.write_all(data)?;
        self.file.flush()?; // guarantees atomic operation. See `new`.

        self.state.advance_position(4 + len as u64);

        // See if you are past the end of the file
        if self.state.is_past_end() {
            // If so, create a new file:
            // Preserves the already allocaed buffer:
            *self.file.get_mut() = OpenOptions::new()
                .create(true)
                .append(true)
                .open(segment_filename(&self.base, self.state.advance_segment()))?;
        }

        Ok(())
    }
}

impl<Ps: Persist> Drop for Sender<Ps> {
    fn drop(&mut self) {
        if let Err(err) = self.persist.save(&self.state) {
            log::error!("could not release sender lock: {}", err);
        }
    }
}

/// The receiver part of the queue.
pub struct Receiver<Ps: Persist> {
    _file_guard: FileGuard,
    tail_follower: TailFollower,
    state: QueueState,
    base: PathBuf,
    persist: Ps,
}

impl<Ps: Persist> Drop for Receiver<Ps> {
    fn drop(&mut self) {
        if let Err(err) = self.persist.save(&self.state) {
            log::error!("could not release receiver lock: {}", err);
        }
    }
}

impl<Ps: Persist> Receiver<Ps> {
    /// Opens a queue for reading. The access will be exclusive, based on the
    /// existence of the temporary file `recv.lock` inside the queue folder.
    pub async fn open<P: AsRef<Path>>(base: P, mut persist: Ps) -> io::Result<Receiver<Ps>> {
        // Guarantee that the queue exists:
        create_dir_all(base.as_ref())?;

        // Acquire guarde and state:
        let file_guard = acquire_recv_lock(base.as_ref())?;
        let state = persist.open_recv(base.as_ref())?;

        // Put the needle on the groove (oh! the 70's):
        let mut tail_follower =
            TailFollower::open(segment_filename(base.as_ref(), state.segment)).await?;
        tail_follower.seek(io::SeekFrom::Start(state.position))?;

        Ok(Receiver {
            _file_guard: file_guard,
            tail_follower,
            state,
            base: PathBuf::from(base.as_ref()),
            persist,
        })
    }

    /// Maybe advance the segment of this receiver.
    async fn maybe_advance(&mut self) -> io::Result<()> {
        // See if you are past the end of the file:
        if self.state.is_past_end() {
            // Advance segment and checkpoint!
            self.state.advance_segment();
            if let Err(err) = self.persist.save(&self.state) {
                self.state.retreat_segment();
                return Err(err);
            }

            // Start listening to new segments:
            self.tail_follower =
                TailFollower::open(segment_filename(&self.base, self.state.segment)).await?;

            // Remove old file:
            remove_file(segment_filename(&self.base, self.state.segment - 1))?;
        }

        Ok(())
    }

    async fn read_one(&mut self) -> io::Result<Vec<u8>> {
        // Read the header to get the length:
        let mut header = [0; 4];
        self.tail_follower.read_exact(&mut header).await?;
        let len = u32::from_be_bytes(header) as usize;

        // With the length, read the data:
        let mut data = (0..len).map(|_| 0).collect::<Vec<_>>();
        self.tail_follower
            .read_exact(&mut data)
            .await
            .expect("poisoned queue");

        Ok(data)
    }

    /// Tries to retrieve an element from the queue. The returned value is a
    /// guard that will only commit state changes to the queue when dropped.
    pub async fn recv(&mut self) -> io::Result<RecvGuard<'_, Ps, Vec<u8>>> {
        self.maybe_advance().await?;
        let data = self.read_one().await?;

        Ok(RecvGuard {
            receiver: self,
            len: 4 + data.len(),
            item: Some(data),
        })
    }

    /// Tries to a number of elements from the queue. The returned value is a
    /// guard that will only commit state changes to the queue when dropped.
    pub async fn recv_batch(&mut self, n: usize) -> io::Result<RecvGuard<'_, Ps, Vec<Vec<u8>>>> {
        let mut data = vec![];

        for _ in 0..n {
            self.maybe_advance().await?;
            data.push(self.read_one().await?);
        }

        Ok(RecvGuard {
            receiver: self,
            len: data.iter().map(|item| 4 + item.len()).sum(),
            item: Some(data),
        })
    }

    /// Tries to a number of elements from the queue. The returned value is a
    /// guard that will only commit state changes to the queue when dropped.
    pub async fn recv_while<P: FnMut(&[u8]) -> bool>(
        &mut self,
        mut predicate: P,
    ) -> io::Result<RecvGuard<'_, Ps, Vec<Vec<u8>>>> {
        self.maybe_advance().await?;
        let mut data = vec![];

        // Poor man's do-while
        loop {
            self.maybe_advance().await?;
            let item = self.read_one().await?;

            if predicate(&item) {
                data.push(item);
            } else {
                break;
            }
        }

        Ok(RecvGuard {
            receiver: self,
            len: data.iter().map(|item| 4 + item.len()).sum(),
            item: Some(data),
        })
    }
}

/// A guard that will only log changes on the queue state when dropped. If it
/// is dropped in a thread that is panicking, no chage will be logged, allowing
/// for the items to be consumed when a new instance recovers. This allows
/// transactional use of the queue.
///
/// This struct implements `Deref` and `DerefMut`. If you realy, realy want
/// ownership, there is `RecvGuard::into_inner`, but be careful.
pub struct RecvGuard<'a, Ps: Persist, T> {
    receiver: &'a mut Receiver<Ps>,
    len: usize,
    item: Option<T>,
}

impl<'a, Ps: Persist, T> Drop for RecvGuard<'a, Ps, T> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.receiver.state.advance_position(self.len as u64);
        }
    }
}

impl<'a, Ps: Persist, T> Deref for RecvGuard<'a, Ps, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.item.as_ref().expect("unreachable")
    }
}

impl<'a, Ps: Persist, T> DerefMut for RecvGuard<'a, Ps, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.item.as_mut().expect("unreachable")
    }
}

impl<'a, Ps: Persist, T> RecvGuard<'a, Ps, T> {
    /// Commits the transaction and returns the underlying value. If you
    /// accedentaly lose this value from now on, it's your own fault!
    pub fn into_inner(mut self) -> T {
        self.item.take().expect("unreachable")
    }

    pub fn commit(self) {
        drop(self);
    }

    pub fn rollback(self) -> io::Result<()> {
        self.receiver
            .tail_follower
            .seek(io::SeekFrom::Current(-(self.len as i64)))
    }
}

/// Convenience function for opening the queue for both sending and receiving.
pub async fn channel<P, Ps>(
    base: P,
    send_persist: Ps,
    recv_persist: Ps,
) -> io::Result<(Sender<Ps>, Receiver<Ps>)>
where
    P: AsRef<Path>,
    Ps: Persist,
{
    Ok((
        Sender::open(base.as_ref(), send_persist)?,
        Receiver::open(base.as_ref(), recv_persist).await?,
    ))
}

/// Deletes a queue at the given path. This function will fail if the queue is
/// in use either for sending or receiving.
pub fn clear<P: AsRef<Path>>(base: P) -> io::Result<()> {
    let _send_lock = acquire_send_lock(base.as_ref())?;
    let _recv_lock = acquire_recv_lock(base.as_ref())?;

    remove_dir(base.as_ref())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_xorshift::XorShiftRng;
    use std::io::Read;
    use std::sync::{Arc, Mutex};

    fn data_lots_of_data() -> impl Iterator<Item = Vec<u8>> {
        let mut rng = XorShiftRng::from_rng(rand::thread_rng()).expect("can init");
        (0..).map(move |_| {
            (0..rng.gen::<usize>() % 128 + 1)
                .map(|_| rng.gen())
                .collect::<Vec<_>>()
        })
    }

    #[test]
    fn enqueue() {
        let mut sender = Sender::open("data/a-queue", FilePersistence::new()).unwrap();
        for data in data_lots_of_data().take(100_000) {
            sender.send(&data).unwrap();
        }
    }

    /// Test enqueuing everything and then dequeueing everything, with no persistence.
    #[test]
    fn enqueue_then_dequeue() {
        // Enqueue:
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();
        let mut sender = Sender::open("data/a-queue", FilePersistence::new()).unwrap();
        for data in &dataset {
            sender.send(data).unwrap();
        }

        // Dequeue:
        futures::executor::block_on(async {
            let mut receiver = Receiver::open("data/a-queue", FilePersistence::new())
                .await
                .unwrap();
            let dataset_iter = dataset.iter();
            let mut i = 0u64;

            for should_be in dataset_iter {
                let data = receiver.recv().await.unwrap();
                assert_eq!(&*data, should_be, "at sample {}", i);
                i += 1;
            }
        });
    }

    /// Test enqueuing and dequeueing, round robin, with no persistence.
    #[test]
    fn enqueue_and_dequeue() {
        // Enqueue:
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();
        let mut sender = Sender::open("data/a-queue", FilePersistence::new()).unwrap();

        let mut i = 0;
        futures::executor::block_on(async {
            let mut receiver = Receiver::open("data/a-queue", FilePersistence::new())
                .await
                .unwrap();
            for data in &dataset {
                sender.send(data).unwrap();
                let received = receiver.recv().await.unwrap();
                assert_eq!(&*received, data, "at sample {}", i);

                i += 1;
            }
        });
    }

    /// Test enqueuing and dequeueing in parallel, with no persistence.
    #[test]
    fn enqueue_dequeue_parallel() {
        // Generate data:
        let dataset = data_lots_of_data().take(10_000_000).collect::<Vec<_>>();
        let arc_sender = Arc::new(dataset);
        let arc_receiver = arc_sender.clone();

        // Enqueue:
        let enqueue = std::thread::spawn(move || {
            let mut sender = Sender::open("data/a-queue", FilePersistence::new()).unwrap();
            for data in &*arc_sender {
                sender.send(data).unwrap();
            }
        });

        // Dequeue:
        let dequeue = std::thread::spawn(move || {
            futures::executor::block_on(async {
                let mut receiver = Receiver::open("data/a-queue", FilePersistence::new())
                    .await
                    .unwrap();
                let dataset_iter = arc_receiver.iter();
                let mut i = 0u64;

                for should_be in dataset_iter {
                    let data = receiver.recv().await.unwrap();
                    assert_eq!(&*data, should_be, "at sample {}", i);
                    i += 1;
                }
            });
        });

        enqueue.join().expect("enqueue thread panicked");
        dequeue.join().expect("dequeue thread panicked");
    }

    #[test]
    fn notify_test() {
        fn read(file: &Mutex<File>) {
            let mut buffer = vec![0; 128];
            let mut lock = file.lock().unwrap();
            loop {
                match lock.read(&mut buffer) {
                    Ok(0) => return,
                    Ok(i) => {
                        print!("{}", String::from_utf8_lossy(&buffer[..i]));
                        // buffer.clear();
                    }
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                        return;
                    }
                    Err(err) => Err(err).unwrap(),
                }
            }
        }

        use notify::event::{Event, EventKind, ModifyKind};
        use notify::Watcher;
        let file = Mutex::new(File::open("data/a-queue/foo.txt").unwrap());

        read(&file);

        let mut watcher =
            notify::immediate_watcher(move |maybe_event: notify::Result<notify::Event>| {
                match maybe_event.unwrap() {
                    Event {
                        kind: EventKind::Modify(ModifyKind::Data(_)),
                        paths,
                        ..
                    } => {
                        let has_changed = paths
                            .into_iter()
                            .any(|path| path.ends_with("data/a-queue/foo.txt"));
                        if has_changed {
                            read(&file);
                        }
                    }
                    _ => {}
                }
            })
            .unwrap();
        watcher
            .watch("data/a-queue", notify::RecursiveMode::NonRecursive)
            .unwrap();

        std::thread::sleep(std::time::Duration::from_secs(60));
    }
}