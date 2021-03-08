//! Synchronization structures based on the filesystem.

use lazy_static::lazy_static;
use notify::RecommendedWatcher;
use rand::Rng;
use std::fs::*;
use std::future::Future;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::watcher::{file_removal_watcher, file_watcher, removal_watcher};

lazy_static! {
    /// A unique token to differentiate between processes wich might have the
    /// same PID, but are otherwise differente instances.
    pub(crate) static ref UNIQUE_PROCESS_TOKEN: u64 = rand::thread_rng().gen();
}

pub fn render_lock() -> String {
    format!(
        "pid={}\ntoken={}",
        std::process::id(),
        *UNIQUE_PROCESS_TOKEN
    )
}

/// A lock using the atomicity of [`OpenOptions::create_new`].
///
/// Be careful! You can easily delete it; just open your file explorer throw
/// it into the trash. It is not the most guaranteed for of atomicity, but it
/// is one standard way of providing a persistent locking mechanism between
/// processes.
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
    /// Ignores errors on the deletion of the guard.
    pub(crate) fn ignore(&mut self) {
        self.ignore = true;
    }

    /// Tries to lock using a certain path in the disk. If the file exists,
    /// returns `Ok(None)`.
    pub fn try_lock<P: AsRef<Path>>(path: P) -> io::Result<Option<FileGuard>> {
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                writeln!(file, "{}", render_lock())?;
                Ok(Some(FileGuard {
                    path: path.as_ref().to_path_buf(),
                    ignore: false,
                }))
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn lock<P: AsRef<Path>>(path: P) -> io::Result<FileGuard> {
        // Set up waker:
        let waker = Arc::new(Mutex::new(None));

        // Set up watcher:
        let _watcher = file_removal_watcher(path.as_ref(), waker.clone());

        Lock { path, waker }.await
    }
}

/// Future for the internals of [`FileGuard::lock`].
struct Lock<P: AsRef<Path>> {
    path: P,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<P: AsRef<Path>> Future for Lock<P> {
    type Output = io::Result<FileGuard>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // Set the waker in the file watcher:
        let mut lock = self.waker.lock().expect("waker mutex poisoned");
        *lock = Some(context.waker().clone());

        match FileGuard::try_lock(self.path.as_ref()) {
            Ok(Some(file_guard)) => Poll::Ready(Ok(file_guard)),
            Ok(None) => Poll::Pending,
            Err(err) => Poll::Ready(Err(err)),
        }
    }
}

/// Follows a file asynchronously. The file needs not to even to exist.
pub struct TailFollower {
    file: io::BufReader<File>,
    read_and_unused: usize,
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
            file: io::BufReader::new(file),
            read_and_unused: 0,
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
        // "Touch" the file and then open it to ensure its existence:
        // Any errors here are OK.
        let maybe_new = OpenOptions::new().create_new(true).append(true).open(&path);

        if maybe_new.is_ok() {
            log::debug!("file `{:?}` didn't exist. Created new", path.as_ref());
        }

        let file = File::open(&path)?;

        Ok(TailFollower::new(path, file))
    }

    pub fn seek(&mut self, seek: io::SeekFrom) -> io::Result<()> {
        self.file.seek(seek).map(|_| ())
    }

    /// Tries to fill the supplied buffer asynchronously. Be careful, since
    /// an EOF (or an interrupted) is considered as "pending". If not enough
    /// data is written to the file, the future will never resolve.
    ///
    /// The returned future operation is _atomic_. If the future is not polled
    /// to completion, the next invocation will rewind to the last position
    /// and start over again.
    ///
    /// # Panics
    ///
    /// This function will panic if unable to seek while rewinding to recover
    /// from an incomplete operation. This may change in the future.
    #[must_use]
    pub fn read_exact<'a>(&'a mut self, buffer: &'a mut [u8]) -> ReadExact<'a> {
        // Rewind if last invocation was not polled to conclusion:
        if self.read_and_unused != 0 {
            log::trace!("found {} bytes read but unused", self.read_and_unused);
            self.seek(io::SeekFrom::Current(-(self.read_and_unused as i64)))
                .expect("could not seek back read and unused bytes");
            self.read_and_unused = 0;
        }

        ReadExact {
            file: &mut self.file,
            buffer,
            waker: &self.waker,
            read_and_unused: &mut self.read_and_unused,
            was_polled: false,
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
//         let mut lock = self.waker.lock().expect("waker mutex poisoned");
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
    file: &'a mut io::BufReader<File>,
    buffer: &'a mut [u8],
    waker: &'a Mutex<Option<Waker>>,
    read_and_unused: &'a mut usize,
    was_polled: bool,
}

impl<'a> ReadExact<'a> {
    fn read_until_you_drain(&mut self) -> Poll<io::Result<()>> {
        log::trace!("reading until drained");
        loop {
            break match self.file.read(&mut self.buffer[*self.read_and_unused..]) {
                Ok(0) => {
                    log::trace!("will have to wait for more");
                    Poll::Pending
                }
                Ok(i) => {
                    log::trace!("read {} bytes", i);
                    *self.read_and_unused += i;
                    if *self.read_and_unused == self.buffer.len() {
                        log::trace!("enough! Done reading");
                        // Now, it is read _and_ used.
                        *self.read_and_unused = 0;
                        Poll::Ready(Ok(()))
                    } else {
                        log::trace!("can read more");
                        continue;
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    log::trace!("got interrupted by eof");
                    Poll::Pending
                }
                Err(err) => {
                    log::trace!("oops! error");
                    Poll::Ready(Err(err))
                }
            };
        }
    }
}

impl<'a> Future for ReadExact<'a> {
    type Output = io::Result<()>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.was_polled = true;
        // See what happens when we read.
        let outcome = self.read_until_you_drain();

        if outcome.is_pending() {
            // Set the waker in the file watcher:
            let mut lock = self.waker.lock().expect("waker mutex poisoned");
            *lock = Some(context.waker().clone());

            // Now, you will have to recheck (TOCTOU!)
            self.read_until_you_drain()
        } else {
            outcome
        }
    }
}

impl<'a> Drop for ReadExact<'a> {
    fn drop(&mut self) {
        if !self.was_polled {
            log::warn!("read_exact future never polled");
        }
    }
}

/// A future that resolves every time that a file is deleted in a directory. This
/// future can be polled over and over again to make a stream of deletions.
pub struct DeletionEvent {
    waker: Arc<Mutex<Option<Waker>>>,
    _watcher: RecommendedWatcher,
}

impl Future for DeletionEvent {
    type Output = ();
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // Set the waker in the file watcher:
        let mut lock = self.waker.lock().expect("waker mutex poisoned");
        *lock = Some(context.waker().clone());

        Poll::Ready(())
    }
}

impl DeletionEvent {
    pub fn new<P: AsRef<Path>>(base: P) -> DeletionEvent {
        let waker = Arc::new(Mutex::new(None));
        let watcher = removal_watcher(base, Arc::clone(&waker));

        DeletionEvent {
            waker,
            _watcher: watcher,
        }
    }
}
