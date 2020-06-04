//! Todo:
//!     * Sender.

mod state;
mod sync;

use std::fs::*;
use std::io::{self, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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

/// The name of the sender lock in the queue folder.
fn send_lock_filename<P: AsRef<Path>>(base: P) -> PathBuf {
    base.as_ref().join("send.lock")
}

/// TODO get rid of this.
struct Inner<T> {
    file: T,
    state: QueueState,
}

/// The sender part of the queue.
pub struct Sender<Ps: Persist> {
    _file_guard: FileGuard,
    file: Arc<Mutex<Inner<io::BufWriter<File>>>>,
    base: PathBuf,
    persist: Ps,
}

impl<Ps: Persist> Sender<Ps> {
    /// Opens a queue for sending.
    pub fn open<P: AsRef<Path>>(base: P, mut persist: Ps) -> io::Result<Sender<Ps>> {
        let file_guard = FileGuard::try_lock(send_lock_filename(base.as_ref()))?
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "foo"))?;

        let state = persist.open_send(base.as_ref())?;
        dbg!(&state);

        // See the docs on OpenOptions::append for why the BufWriter here.
        let file = io::BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(segment_filename(base.as_ref(), state.segment))?,
        );

        Ok(Sender {
            _file_guard: file_guard,
            file: Arc::new(Mutex::new(Inner { file, state })),
            base: PathBuf::from(base.as_ref()),
            persist,
        })
    }

    /// Sends some data into the queue. One send is always atomic.
    pub fn send(&self, data: &[u8]) -> io::Result<()> {
        // Get length of the data and write the header:
        let len = data.len();
        assert!(len < std::u32::MAX as usize);
        let header = (len as u32).to_be_bytes();

        // Acquire lock and write:
        let mut lock = self.file.lock().expect("poisoned");

        // See if you are past the end of the file
        if lock.state.is_past_end() {
            // If so, create a new file:
            // Preserves the already allocaed buffer:
            *lock.file.get_mut() = OpenOptions::new()
                .create(true)
                .append(true)
                .open(segment_filename(&self.base, lock.state.advance_segment()))?;
        }

        lock.file.write_all(&header)?;
        lock.file.write_all(data)?;
        lock.file.flush()?; // guarantees atomic operation. See `new`.

        lock.state.advance_position(4 + len as u64);

        Ok(())
    }
}

impl<Ps: Persist> Drop for Sender<Ps> {
    fn drop(&mut self) {
        println!("saving sender {:?}", self.file.lock().unwrap().state);
        self.persist
            .save(&self.file.lock().expect("poisoned").state);
    }
}

/// The receiver part of the queue.
pub struct Receiver<Ps: Persist> {
    _file_guard: FileGuard,
    tail_follower: Inner<TailFollower>,
    base: PathBuf,
    persist: Ps,
}

impl<Ps: Persist> Drop for Receiver<Ps> {
    fn drop(&mut self) {
        println!("saving recv {:?}", self.tail_follower.state);
        self.persist.save(&self.tail_follower.state);
    }
}

impl<Ps: Persist> Receiver<Ps> {
    /// Opens a queue for reading. The access will be exclusive, based on the
    /// existence of the temporary file `recv.lock` inside the queue folder.
    pub async fn open<P: AsRef<Path>>(base: P, mut persist: Ps) -> io::Result<Receiver<Ps>> {
        let file_guard = FileGuard::try_lock(recv_lock_filename(base.as_ref()))?
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "foo"))?;

        let state = persist.open_recv(base.as_ref())?;
        let file = TailFollower::open(segment_filename(base.as_ref(), state.segment)).await?;

        Ok(Receiver {
            _file_guard: file_guard,
            tail_follower: Inner { file, state },
            base: PathBuf::from(base.as_ref()),
            persist,
        })
    }

    /// Tries to retrieve an element from the queue. The returned value is a
    /// guard that will only commit state changes to the queue when dropped.
    pub async fn recv(&mut self) -> io::Result<RecvGuard<'_, Ps, Vec<u8>>> {
        // Read the header to get the length:
        let mut header = [0; 4];
        self.tail_follower.file.read_exact(&mut header).await?;
        let len = u32::from_be_bytes(header) as usize;

        // See if you are past the end if the file:
        if self.tail_follower.state.is_past_end() {
            // Advance segment and checkpoint!
            self.tail_follower.state.advance_segment();
            self.persist
                .save(&self.tail_follower.state)
                .map_err(|err| {
                    self.tail_follower.state.retreat_segment();
                    err
                })?; // checkpoint!

            self.tail_follower.file = TailFollower::open(segment_filename(
                &self.base,
                self.tail_follower.state.segment,
            ))
            .await?;

            remove_file(segment_filename(
                &self.base,
                self.tail_follower.state.segment - 1,
            ))?;
        }

        // With the length, read the data:
        let mut data = (0..len).map(|_| 0).collect::<Vec<_>>();
        self.tail_follower
            .file
            .read_exact(&mut data)
            .await
            .expect("poisoned queue");

        Ok(RecvGuard {
            receiver: self,
            len,
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
            self.receiver
                .tail_follower
                .state
                .advance_position(4 + self.len as u64);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_xorshift::XorShiftRng;
    use std::io::Read;

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
        let sender = Sender::open("data/a-queue", FilePersistence::new()).unwrap();
        for data in data_lots_of_data().take(100_000) {
            sender.send(&data).unwrap();
        }
    }

    /// Test enqueuing everything and then dequeueing everything, with no persistence.
    #[test]
    fn enqueue_then_dequeue() {
        // Enqueue:
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();
        let sender = Sender::open("data/a-queue", FilePersistence::new()).unwrap();
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
        let sender = Sender::open("data/a-queue", FilePersistence::new()).unwrap();

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
        let dataset = data_lots_of_data().take(1_000_000).collect::<Vec<_>>();
        let arc_sender = Arc::new(dataset);
        let arc_receiver = arc_sender.clone();

        // Enqueue:
        let enqueue = std::thread::spawn(move || {
            let sender = Sender::open("data/a-queue", FilePersistence::new()).unwrap();
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
