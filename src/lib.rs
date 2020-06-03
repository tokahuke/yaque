mod sync;

use std::fs::*;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use sync::TailFollower;

const MIN_SEGMENT_SIZE: u64 = 1024 * 32; // 32MB

fn filename<P: AsRef<Path>>(base: P, segment: u64) -> PathBuf {
    base.as_ref().join(format!("{}.q", segment))
}

#[derive(Debug, PartialEq, Default)]
pub struct QueueState {
    segment: u64,
    position: u64,
}

impl QueueState {
    fn advance_segment(&mut self) -> u64 {
        self.position = 0;
        self.segment += 1;
        self.segment
    }

    fn advance_position(&mut self, offset: u64) -> bool {
        self.position += offset;
        self.position > MIN_SEGMENT_SIZE
    }
}

struct Inner<T> {
    file: T,
    state: QueueState,
}

pub struct Sender {
    file: Arc<Mutex<Inner<io::BufWriter<File>>>>,
    base: PathBuf,
}

impl Sender {
    pub fn open<P: AsRef<Path>>(base: P, state: QueueState) -> io::Result<Sender> {
        // See the docs on OpenOptions::append for why the BufWriter here.
        let file = io::BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(filename(base.as_ref(), state.segment))?,
        );

        Ok(Sender {
            file: Arc::new(Mutex::new(Inner { file, state })),
            base: PathBuf::from(base.as_ref()),
        })
    }

    pub fn send(&self, data: &[u8]) -> io::Result<()> {
        // Get length of the data and write the header:
        let len = data.len();
        assert!(len < std::u32::MAX as usize);
        let header = (len as u32).to_be_bytes();

        // Acquire lock and write:
        let mut lock = self.file.lock().expect("poisoned");
        lock.file.write_all(&header)?;
        lock.file.write_all(data)?;
        lock.file.flush()?; // guarantees atomic operation. See `new`.

        // With the lock, update state:
        if lock.state.advance_position(4 + len as u64) {
            // Preserves the already allocaed buffer:
            *lock.file.get_mut() = OpenOptions::new()
                .create(true)
                .append(true)
                .open(filename(&self.base, lock.state.advance_segment()))?;
        }

        Ok(())
    }
}

pub struct Receiver {
    tail_follower: Inner<TailFollower>,
    base: PathBuf,
}

impl Receiver {
    pub fn open<P: AsRef<Path>>(base: P, state: QueueState) -> io::Result<Receiver> {
        let file = TailFollower::open(filename(base.as_ref(), state.segment))?;

        Ok(Receiver {
            tail_follower: Inner { file, state },
            base: PathBuf::from(base.as_ref()),
        })
    }

    pub async fn recv(&mut self) -> io::Result<Vec<u8>> {
        // Read the header to get the length:
        let mut header = [0; 4];
        self.tail_follower.file.read_exact(&mut header).await?;
        let len = u32::from_be_bytes(header) as usize;

        // With the length, read the data:
        let mut data = (0..len).map(|_| 0).collect::<Vec<_>>();
        self.tail_follower
            .file
            .read_exact(&mut data)
            .await
            .expect("poisoned queue");

        // With the lock, update state:
        if self.tail_follower.state.advance_position(4 + len as u64) {
            self.tail_follower.file = TailFollower::open(filename(
                &self.base,
                self.tail_follower.state.advance_segment(),
            ))?;

            remove_file(filename(&self.base, self.tail_follower.state.segment - 1))?;
        }

        Ok(data)
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
        let sender = Sender::open("data/a-queue", QueueState::default()).unwrap();
        for data in data_lots_of_data().take(100_000) {
            sender.send(&data).unwrap();
        }
    }

    /// Test enqueuing everything and then dequeueing everything, with no persistence.
    #[test]
    fn enqueue_then_dequeue() {
        // Enqueue:
        let dataset = data_lots_of_data().take(1_000_000).collect::<Vec<_>>();
        let sender = Sender::open("data/a-queue", QueueState::default()).unwrap();
        for data in &dataset {
            sender.send(data).unwrap();
        }

        // Dequeue:
        let mut receiver = Receiver::open("data/a-queue", QueueState::default()).unwrap();
        let dataset_iter = dataset.iter();
        let mut i = 0;

        futures::executor::block_on(async {
            for should_be in dataset_iter {
                let data = receiver.recv().await.unwrap();
                assert_eq!(data, *should_be, "at sample {}", i);
                i += 1;
            }
        });
    }

    /// Test enqueuing and dequeueing, round robin, with no persistence.
    #[test]
    fn enqueue_and_dequeue() {
        // Enqueue:
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();
        let sender = Sender::open("data/a-queue", QueueState::default()).unwrap();
        let mut receiver = Receiver::open("data/a-queue", QueueState::default()).unwrap();

        let mut i = 0;
        futures::executor::block_on(async {
            for data in &dataset {
                sender.send(data).unwrap();
                let received = receiver.recv().await.unwrap();
                assert_eq!(received, *data, "at sample {}", i);

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
        let sender = Sender::open("data/a-queue", QueueState::default()).unwrap();

        // Enqueue:
        let enqueue = std::thread::spawn(move || {
            for data in &*arc_sender {
                sender.send(data).unwrap();
            }
        });

        // Dequeue:
        let dequeue = std::thread::spawn(move || {
            let mut receiver = Receiver::open("data/a-queue", QueueState::default()).unwrap();
            let dataset_iter = arc_receiver.iter();
            let mut i = 0;

            futures::executor::block_on(async {
                for should_be in dataset_iter {
                    let data = receiver.recv().await.unwrap();
                    assert_eq!(data, *should_be, "at sample {}", i);
                }

                i += 1;
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
