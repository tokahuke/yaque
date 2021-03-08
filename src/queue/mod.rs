//! Queue implementation and utility functions.

mod receiver;
mod sender;

pub use receiver::{Receiver, RecvGuard};
pub use sender::{Sender, SenderBuilder};

pub(crate) use receiver::recv_lock_filename;
pub(crate) use sender::send_lock_filename;

use std::fs::*;
use std::io::{self};
use std::path::{Path, PathBuf};

use receiver::{acquire_recv_lock, try_acquire_recv_lock};
use sender::{acquire_send_lock, try_acquire_send_lock};

/// The name of segment file in the queue folder.
fn segment_filename<P: AsRef<Path>>(base: P, segment: u64) -> PathBuf {
    base.as_ref().join(format!("{}.q", segment))
}

/// The value of a header EOF.
const HEADER_EOF: [u8; 4] = [255, 255, 255, 255];

/// Convenience function for opening the queue for both sending and receiving.
pub fn channel<P: AsRef<Path>>(base: P) -> io::Result<(Sender, Receiver)> {
    Ok((Sender::open(base.as_ref())?, Receiver::open(base.as_ref())?))
}

/// Tries to deletes a queue at the given path. This function will fail if the
/// queue is in use either for sending or receiving.
pub fn try_clear<P: AsRef<Path>>(base: P) -> io::Result<()> {
    let mut send_lock = try_acquire_send_lock(base.as_ref())?;
    let mut recv_lock = try_acquire_recv_lock(base.as_ref())?;

    // Sets the the locks to ignore when their files magically disappear.
    send_lock.ignore();
    recv_lock.ignore();

    remove_dir_all(base.as_ref())?;

    Ok(())
}

/// Deletes a queue at the given path. This function will await the queue to
/// become available for both sending and receiving.
pub async fn clear<P: AsRef<Path>>(base: P) -> io::Result<()> {
    let mut send_lock = acquire_send_lock(base.as_ref()).await?;
    let mut recv_lock = acquire_recv_lock(base.as_ref()).await?;

    // Sets the the locks to ignore when their files magically disappear.
    send_lock.ignore();
    recv_lock.ignore();

    remove_dir_all(base.as_ref())?;

    Ok(())
}

/// Global initialization for tests
#[cfg(test)]
#[ctor::ctor]
fn init_log() {
    // Init logger:
    #[cfg(feature = "log-trace")]
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Trace)
        .init()
        .ok();

    #[cfg(feature = "log-debug")]
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Debug)
        .init()
        .ok();

    // Remove an old test:
    std::fs::remove_dir_all("data").ok();

    // Create new structure:
    std::fs::create_dir_all("data").unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures_timer::Delay;
    use rand::{Rng, SeedableRng};
    use rand_xorshift::XorShiftRng;
    use std::sync::Arc;
    use std::time::Duration;

    fn data_lots_of_data() -> impl Iterator<Item = Vec<u8>> {
        let mut rng = XorShiftRng::from_rng(rand::thread_rng()).expect("can init");
        (0..).map(move |_| {
            (0..rng.gen::<usize>() % 128 + 1)
                .map(|_| rng.gen())
                .collect::<Vec<_>>()
        })
    }

    #[test]
    fn create_and_clear() {
        let _ = Sender::open("data/create-and-clear").unwrap();
        try_clear("data/create-and-clear").unwrap();
    }

    #[test]
    #[should_panic]
    fn create_and_clear_fails() {
        let sender = Sender::open("data/create-and-clear-fails").unwrap();
        try_clear("data/create-and-clear-fails").unwrap();
        drop(sender);
    }

    #[test]
    fn create_and_clear_async() {
        let _ = Sender::open("data/create-and-clear-async").unwrap();

        futures::executor::block_on(async { clear("data/create-and-clear-async").await.unwrap() });
    }

    #[test]
    fn test_enqueue() {
        let mut sender = Sender::open("data/enqueue").unwrap();
        for data in data_lots_of_data().take(100_000) {
            sender.try_send(&data).unwrap();
        }
    }

    /// Test enqueuing everything and then dequeueing everything, with no persistence.
    #[test]
    fn test_enqueue_then_dequeue() {
        // Enqueue:
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();
        let mut sender = Sender::open("data/enqueue-then-dequeue").unwrap();
        for data in &dataset {
            sender.try_send(data).unwrap();
        }

        log::trace!("enqueued");

        // Dequeue:
        futures::executor::block_on(async {
            let mut receiver = Receiver::open("data/enqueue-then-dequeue").unwrap();
            let dataset_iter = dataset.iter();
            let mut i = 0u64;

            for should_be in dataset_iter {
                let data = receiver.recv().await.unwrap();
                assert_eq!(&*data, should_be, "at sample {}", i);
                i += 1;
                data.commit();
            }
        });
    }

    /// Test enqueuing and dequeueing, round robin, with no persistence.
    #[test]
    fn test_enqueue_and_dequeue() {
        // Enqueue:
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();
        let mut sender = Sender::open("data/enqueue-and-dequeue").unwrap();

        futures::executor::block_on(async {
            let mut receiver = Receiver::open("data/enqueue-and-dequeue").unwrap();
            let mut i = 0;

            for data in &dataset {
                sender.try_send(data).unwrap();
                let received = receiver.recv().await.unwrap();
                assert_eq!(&*received, data, "at sample {}", i);

                i += 1;
                received.commit();
            }
        });
    }

    /// Test enqueuing and dequeueing in parallel.
    #[test]
    fn test_enqueue_dequeue_parallel() {
        // Generate data:
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();
        let arc_sender = Arc::new(dataset);
        let arc_receiver = arc_sender.clone();

        // Enqueue:
        let enqueue = std::thread::spawn(move || {
            let mut sender = Sender::open("data/enqueue-dequeue-parallel").unwrap();
            for data in &*arc_sender {
                sender.try_send(data).unwrap();
            }
        });

        // Dequeue:
        let dequeue = std::thread::spawn(move || {
            futures::executor::block_on(async {
                let mut receiver = Receiver::open("data/enqueue-dequeue-parallel").unwrap();
                let dataset_iter = arc_receiver.iter();
                let mut i = 0u64;

                for should_be in dataset_iter {
                    let data = receiver.recv().await.unwrap();
                    assert_eq!(&*data, should_be, "at sample {}", i);
                    i += 1;
                    data.commit();
                }
            });
        });

        enqueue.join().expect("enqueue thread panicked");
        dequeue.join().expect("dequeue thread panicked");
    }

    /// Test enqueuing and dequeueing in parallel, using batches.
    #[test]
    fn test_enqueue_dequeue_parallel_with_batches() {
        // Generate data:
        let mut dataset = vec![];
        let mut batch = vec![];

        for data in data_lots_of_data().take(100_000) {
            batch.push(data);

            if batch.len() >= 256 {
                dataset.push(batch);
                batch = vec![];
            }
        }

        let arc_sender = Arc::new(dataset);
        let arc_receiver = arc_sender.clone();

        // Enqueue:
        let enqueue = std::thread::spawn(move || {
            let mut sender = Sender::open("data/enqueue-dequeue-parallel-with-batches").unwrap();
            for batch in &*arc_sender {
                sender.try_send_batch(batch).unwrap();
            }
        });

        // Dequeue:
        let dequeue = std::thread::spawn(move || {
            futures::executor::block_on(async {
                let mut receiver =
                    Receiver::open("data/enqueue-dequeue-parallel-with-batches").unwrap();
                let dataset_iter = arc_receiver.iter();
                let mut i = 0u64;

                for should_be in dataset_iter {
                    let batch = receiver.recv_batch(256).await.unwrap();
                    assert_eq!(&*batch, should_be, "at sample {}", i);
                    i += 1;
                    batch.commit();
                }
            });
        });

        enqueue.join().expect("enqueue thread panicked");
        dequeue.join().expect("dequeue thread panicked");
    }

    #[test]
    fn test_dequeue_is_atomic() {
        let mut sender = Sender::open("data/dequeue-is-atomic").unwrap();
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();

        futures::executor::block_on(async move {
            let mut receiver = Receiver::open("data/dequeue-is-atomic").unwrap();
            let dataset_iter = dataset.iter();
            let mut i = 0u64;

            for data in dataset_iter {
                sender.try_send(data).unwrap();
                // Try not to poll the future to the end.
                // TODO maybe you need something a bit more convincing than
                // `async {}`...
                let incomplete =
                    futures::future::select(Box::pin(receiver.recv()), Box::pin(async {})).await;
                drop(incomplete); // need to force this explicitly.

                //
                let received = receiver.recv().await.unwrap();
                assert_eq!(&*received, data, "at sample {}", i);
                i += 1;
                received.commit();
            }
        });
    }

    #[test]
    fn test_rollback() {
        futures::executor::block_on(async move {
            let (mut sender, mut receiver) = channel("data/rollback").unwrap();
            sender.try_send(b"123").unwrap();
            sender.try_send(b"456").unwrap();

            assert_eq!(&*receiver.recv().await.unwrap(), b"123");
            assert_eq!(&*receiver.recv().await.unwrap(), b"123");

            receiver.recv().await.unwrap().commit();

            assert_eq!(&*receiver.recv().await.unwrap(), b"456");
            assert_eq!(&*receiver.recv().await.unwrap(), b"456");
        });
    }

    #[test]
    fn test_recv_timeout_nothing() {
        futures::executor::block_on(async move {
            let (_, mut receiver) = channel("data/recv-timeout-nothing").unwrap();

            assert!(receiver
                .recv_timeout(Delay::new(Duration::from_secs(1)))
                .await
                .unwrap()
                .is_none(),);
        });
    }

    #[test]
    fn test_recv_timeout_immediate() {
        futures::executor::block_on(async move {
            let (mut sender, mut receiver) = channel("data/recv-timeout-immediate").unwrap();

            sender.try_send(b"123").unwrap();
            // sender.send(b"456").unwrap();

            assert_eq!(
                &*receiver
                    .recv_timeout(Delay::new(Duration::from_secs(1)))
                    .await
                    .unwrap()
                    .unwrap(),
                b"123"
            );
        });
    }

    #[test]
    fn test_recv_timeout_dealyed() {
        futures::executor::block_on(async move {
            let (mut sender, mut receiver) = channel("data/recv-timeout-delayed").unwrap();

            std::thread::spawn(move || {
                futures::executor::block_on(async move {
                    Delay::new(Duration::from_secs(1)).await;
                    sender.try_send(b"123").unwrap();
                });
            });

            assert_eq!(
                &*receiver
                    .recv_timeout(Delay::new(Duration::from_secs(2)))
                    .await
                    .unwrap()
                    .unwrap(),
                b"123"
            );
        });
    }

    #[test]
    fn test_recv_batch_timeout_nothing() {
        futures::executor::block_on(async move {
            let (_, mut receiver) = channel("data/recv-batch-timeout-nothing").unwrap();

            assert!(receiver
                .recv_batch_timeout(2, Delay::new(Duration::from_secs(1)))
                .await
                .unwrap()
                .is_empty(),);
        });
    }

    #[test]
    fn test_recv_batch_timeout_immediate() {
        futures::executor::block_on(async move {
            let (mut sender, mut receiver) = channel("data/recv-batch-timeout-immediate").unwrap();

            sender.try_send(b"123").unwrap();
            sender.try_send(b"456").unwrap();

            assert_eq!(
                &*receiver
                    .recv_batch_timeout(2, Delay::new(Duration::from_secs(1)))
                    .await
                    .unwrap(),
                &[b"123", b"456"],
            );
        });
    }

    #[test]
    fn test_recv_batch_timeout_dealyed_1() {
        futures::executor::block_on(async move {
            let (mut sender, mut receiver) = channel("data/recv-batch-timeout-delayed-1").unwrap();

            std::thread::spawn(move || {
                futures::executor::block_on(async move {
                    for i in 0..5usize {
                        Delay::new(Duration::from_secs_f64(0.5)).await;
                        sender.try_send(i.to_string().as_bytes()).unwrap();
                    }
                });
            });

            assert_eq!(
                &*receiver
                    .recv_batch_timeout(3, Delay::new(Duration::from_secs(2)))
                    .await
                    .unwrap(),
                &[b"0", b"1", b"2"]
            );
        });
    }

    #[test]
    fn test_recv_batch_timeout_dealyed_2() {
        futures::executor::block_on(async move {
            let (mut sender, mut receiver) = channel("data/recv-batch-timeout-delayed-2").unwrap();

            std::thread::spawn(move || {
                futures::executor::block_on(async move {
                    for i in 0..5usize {
                        Delay::new(Duration::from_secs_f64(0.6)).await;
                        sender.try_send(i.to_string().as_bytes()).unwrap();
                    }
                });
            });

            assert_eq!(
                &*receiver
                    .recv_batch_timeout(5, Delay::new(Duration::from_secs(2)))
                    .await
                    .unwrap(),
                &[b"0", b"1", b"2"]
            );
        });
    }
}
