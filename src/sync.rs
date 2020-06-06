//! Synchronization structures based on the filesystem.

use notify::event::{Event, EventKind, ModifyKind};
use notify::{RecommendedWatcher, Watcher};
use std::fs::*;
use std::future::Future;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// A lock using the atomicity of `OpenOptions::create_new`. Not exactly a good
/// lock. You can easly delete it and everything goes down the drain.
pub struct FileGuard {
    path: PathBuf,
    ignore: bool,
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        if let Err(err) = remove_file(&self.path) {
            if !self.ignore {
                log::error!("unable to drop file lock: {}", err);
                return;
            }
        }

        log::trace!("file guard on `{:?}` dropped", self.path);
    }
}

impl FileGuard {
    /// Igonres errors on the deletion of the guard.
    pub(crate) fn ignore(&mut self) {
        self.ignore = true;
    }

    /// Tries to lock using a certain path in the disk. If the file exists,
    /// returns `Ok(None)`.
    pub fn try_lock<P: AsRef<Path>>(path: P) -> io::Result<Option<FileGuard>> {
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                writeln!(file, "pid={}", std::process::id())?;
                Ok(Some(FileGuard {
                    path: path.as_ref().to_path_buf(),
                    ignore: false,
                }))
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(None),
            Err(err) => Err(err),
        }
    }
}

// /// Watches for the creation of a given future file.
// fn file_creation_watcher<P>(path: P, waker: Arc<Mutex<Option<Waker>>>) -> RecommendedWatcher
// where
//     P: AsRef<Path>,
// {
//     // Set up watcher:
//     let mut watcher =
//         notify::immediate_watcher(move |maybe_event: notify::Result<notify::Event>| {
//             match maybe_event.expect("received error from watcher") {
//                 // When any modificatin in the file happens
//                 Event {
//                     kind: EventKind::Create(_),
//                     ..
//                 } => {
//                     waker
//                         .lock()
//                         .expect("waker poisoned")
//                         .as_mut()
//                         .map(|waker: &mut Waker| waker.wake_by_ref());
//                 }
//                 _ => {}
//             }
//         })
//         .expect("could not create watcher");

//     // Put watcher to run:
//     watcher
//         .watch(
//             path.as_ref().parent().expect("file must have parent"),
//             notify::RecursiveMode::NonRecursive,
//         )
//         .expect("could not start watching file");

//     watcher
// }

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
                    log::debug!("file being watched was removed");
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

    /// Tries to open a file for reading, creating it, if necessary. This is
    /// not atomic: someone might sneak in just in the right moment and delete
    /// the file before we open it for reading. To prevent this, use a lockfile.
    pub fn open<P>(path: P) -> io::Result<TailFollower>
    where
        P: 'static + AsRef<Path> + Send + Sync,
    {
        // // Set up creation waker:
        // let waker = Arc::new(Mutex::new(None));

        // // Set up creation watcher:
        // let _creation_watcher = file_creation_watcher(&path, waker.clone());

        // "Touch" the file and then open it to ensure its existence:
        // Any errors here are OK.
        let maybe_new = OpenOptions::new().create_new(true).append(true).open(&path);

        if maybe_new.is_ok() {
            log::debug!("file `{:?}` didn't exist. Created new", path.as_ref());
        }

        // Someone might snear
        let file = File::open(&path)?;

        Ok(TailFollower::new(path, file))
    }

    pub fn seek(&mut self, seek: io::SeekFrom) -> io::Result<()> {
        self.file.seek(seek).map(|_| ())
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

// /// A future to the opening of a file. This future will resolve immediately if
// /// the file exists or await the file creation.
// struct Open<'a, P> {
//     waker: &'a Mutex<Option<Waker>>,
//     path: &'a P,
// }

// impl<'a, P: 'static + AsRef<Path> + Send + Sync> Future for Open<'a, P> {
//     type Output = io::Result<File>;
//     fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
//         // Set the waker in the file watcher:
//         let mut lock = self.waker.lock().expect("waker mutex posoned");
//         *lock = Some(context.waker().clone());

//         match File::open(self.path.as_ref()) {
//             Ok(file) => Poll::Ready(Ok(file)),
//             Err(err) if err.kind() == io::ErrorKind::NotFound => Poll::Pending,
//             Err(err) => Poll::Ready(Err(err)),
//         }
//     }
// }

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
