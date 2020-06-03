use notify::event::{Event, EventKind, ModifyKind};
use notify::{RecommendedWatcher, Watcher};
use std::fs::*;
use std::future::Future;
use std::io::{self, Read};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

fn file_watcher<P>(path: Arc<P>, waker: Arc<Mutex<Option<Waker>>>) -> RecommendedWatcher
where
    P: 'static + AsRef<Path> + Send + Sync,
{
    let path_arc = Arc::clone(&path);

    // Set up watcher:
    let mut watcher =
        notify::immediate_watcher(move |maybe_event: notify::Result<notify::Event>| {
            match maybe_event.unwrap() {
                // When any modificatin in the file happens
                Event {
                    kind: EventKind::Modify(ModifyKind::Data(_)),
                    paths,
                    ..
                } => {
                    // See if our file has changed:
                    let has_changed = paths
                        .into_iter()
                        .any(|a_path| a_path.ends_with(path.as_ref()));

                    // If it indeed has changed:
                    if has_changed {
                        waker
                            .lock()
                            .expect("waker poisoned")
                            .as_mut()
                            .map(|waker: &mut Waker| waker.wake_by_ref());
                    }
                }
                Event {
                    kind: EventKind::Remove(_),
                    paths,
                    ..
                } => {
                    // See if our file has changed:
                    let was_deleted = paths
                        .into_iter()
                        .any(|a_path| a_path.ends_with(path.as_ref()));

                    if was_deleted {
                        println!("file removed");
                    }
                }
                _ => {}
            }
        })
        .expect("could not create watcher");

    // Put watcher to run:
    watcher
        .watch(path_arc.as_ref(), notify::RecursiveMode::NonRecursive)
        .expect("could not start watching file");

    watcher
}

pub struct TailFollower {
    file: File,
    _watcher: RecommendedWatcher,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl TailFollower {
    pub fn open<P>(path: P) -> io::Result<TailFollower>
    where
        P: 'static + AsRef<Path> + Send + Sync,
    {
        let path_arc = Arc::new(path);
        let waker = Arc::new(Mutex::new(None));

        // Open file:
        let file = File::open(path_arc.as_ref())?;

        // Set up watcher:
        let watcher = file_watcher(path_arc.clone(), waker.clone());

        Ok(TailFollower {
            file,
            _watcher: watcher,
            waker,
        })
    }

    pub fn read_exact<'a>(&'a mut self, buffer: &'a mut [u8]) -> ReadExact<'a> {
        ReadExact {
            file: &mut self.file,
            buffer,
            waker: &self.waker,
            filled: 0,
        }
    }
}

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

    /// TODO: make it stop!
    #[test]
    fn tail_follow_test() {
        let mut tail_follower = TailFollower::open("data/foo.txt").unwrap();

        futures::executor::block_on(async move {
            let mut buffer = vec![0; 128];

            loop {
                tail_follower.read_exact(&mut buffer).await.unwrap();
                print!("{}", String::from_utf8_lossy(&mut buffer));
                buffer = vec![0; 128];
            }
        });
    }
}
