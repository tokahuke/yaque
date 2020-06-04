use notify::event::{Event, EventKind, ModifyKind};
use notify::{RecommendedWatcher, Watcher};
use std::fs::*;
use std::future::Future;
use std::io::{self, Read};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// Watches for the creation of a given future file.
fn file_creation_watcher<P>(path: P, waker: Arc<Mutex<Option<Waker>>>) -> RecommendedWatcher
where
    P: AsRef<Path>,
{
    // Set up watcher:
    let mut watcher =
        notify::immediate_watcher(move |maybe_event: notify::Result<notify::Event>| {
            match maybe_event.expect("received error from watcher") {
                // When any modificatin in the file happens
                Event {
                    kind: EventKind::Create(_),
                    ..
                } => {
                    waker
                        .lock()
                        .expect("waker poisoned")
                        .as_mut()
                        .map(|waker: &mut Waker| waker.wake_by_ref());
                }
                _ => {}
            }
        })
        .expect("could not create watcher");

    // Put watcher to run:
    watcher
        .watch(
            path.as_ref().parent().expect("file must have parent"),
            notify::RecursiveMode::NonRecursive,
        )
        .expect("could not start watching file");

    watcher
}

/// Watches a file for changes in its content.
fn file_watcher<P>(path: P, waker: Arc<Mutex<Option<Waker>>>) -> RecommendedWatcher
where
    P: AsRef<Path>,
{
    // Set up watcher:
    let mut watcher =
        notify::immediate_watcher(move |maybe_event: notify::Result<notify::Event>| {
            match maybe_event.expect("received error from watcher") {
                // When any modificatin in the file happens
                Event {
                    kind: EventKind::Modify(ModifyKind::Data(_)),
                    ..
                } => {
                    waker
                        .lock()
                        .expect("waker poisoned")
                        .as_mut()
                        .map(|waker: &mut Waker| waker.wake_by_ref());
                }
                Event {
                    kind: EventKind::Remove(_),
                    ..
                } => {
                    log::info!("file removed");
                }
                _ => {}
            }
        })
        .expect("could not create watcher");

    // Put watcher to run:
    watcher
        .watch(path, notify::RecursiveMode::NonRecursive)
        .expect("could not start watching file");

    watcher
}

/// Follows a file assynchronously. The file needs not to even to exist.
pub struct TailFollower {
    file: File,
    _watcher: RecommendedWatcher,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl TailFollower {
    /// Creates a new following file.
    fn new<P>(path: P, file: File) -> TailFollower
    where
        P: 'static + AsRef<Path> + Send + Sync,
    {
        // Set up waker:
        let waker = Arc::new(Mutex::new(None));

        // Set up watcher:
        let watcher = file_watcher(path, waker.clone());

        TailFollower {
            file,
            _watcher: watcher,
            waker,
        }
    }

    /// Tries to open a file, awaitng for its creation, if necessary.
    pub async fn open<P>(path: P) -> io::Result<TailFollower>
    where
        P: 'static + AsRef<Path> + Send + Sync,
    {
        // Set up creation waker:
        let waker = Arc::new(Mutex::new(None));

        // Set up creation watcher:
        let _creation_watcher = file_creation_watcher(&path, waker.clone());

        // Open file:
        let file = Open {
            waker: &*waker,
            path: &path,
        }
        .await?;

        Ok(TailFollower::new(path, file))
    }

    /// Tries to fill the supplied buffer assynchronously. Be carefull, since
    /// an EOF (or an interrupted) is considered as "pending". If no enough
    /// data is written to the file, the future will never resolve.
    pub fn read_exact<'a>(&'a mut self, buffer: &'a mut [u8]) -> ReadExact<'a> {
        ReadExact {
            file: &mut self.file,
            buffer,
            waker: &self.waker,
            filled: 0,
        }
    }
}

/// A future to the opening of a file. This future will resolve immediately if
/// the file exists or await the file creation.
struct Open<'a, P> {
    waker: &'a Mutex<Option<Waker>>,
    path: &'a P,
}

impl<'a, P: 'static + AsRef<Path> + Send + Sync> Future for Open<'a, P> {
    type Output = io::Result<File>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // Set the waker in the file watcher:
        let mut lock = self.waker.lock().expect("waker mutex posoned");
        *lock = Some(context.waker().clone());

        match File::open(self.path.as_ref()) {
            Ok(file) => Poll::Ready(Ok(file)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Poll::Pending,
            Err(err) => Poll::Ready(Err(err)),
        }
    }
}

/// The future returned by `TailFollower::read_exact`.
pub struct ReadExact<'a> {
    file: &'a mut File,
    buffer: &'a mut [u8],
    waker: &'a Mutex<Option<Waker>>,
    filled: usize,
}

impl<'a> Future for ReadExact<'a> {
    type Output = io::Result<()>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // Set the waker in the file watcher:
        let mut lock = self.waker.lock().expect("waker mutex posoned");
        *lock = Some(context.waker().clone());

        // Now, get the slice.
        let self_mut = &mut *self; // tricky Pins!!! Need this to guide the borrow checker.

        // Now see what happens when we read.
        match self_mut.file.read(&mut self_mut.buffer[self_mut.filled..]) {
            Ok(0) => Poll::Pending,
            Ok(i) => {
                self.filled += i;
                if self.filled == self.buffer.len() {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => Poll::Pending,
            Err(err) => Poll::Ready(Err(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TODO: make it stop! Make it stop!!
    #[test]
    fn tail_follow_test() {
        futures::executor::block_on(async move {
            let mut tail_follower = TailFollower::open("data/foo.txt").await.unwrap();
            let mut buffer = vec![0; 128];

            loop {
                tail_follower.read_exact(&mut buffer).await.unwrap();
                print!("{}", String::from_utf8_lossy(&mut buffer));
                buffer = vec![0; 128];
            }
        });
    }
}
