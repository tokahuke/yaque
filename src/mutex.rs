//! A persistent mutex implementation using the atomicity of [`OpenOptions::create_new`]
//!
//! Please note that this `Mutex` just really works if other processeses in your system
//! are willing to "play nice" with you. In most systems (Unix-like), locks are mostly
//! advisory.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

pub use crate::sync::FileGuard;

/// A persistent mutex implementation using the atomicity of [`OpenOptions::create_new`].
/// This structure, as opposed to [`FileGuard`], holds some content in a separate file.
pub struct Mutex {
    path: PathBuf,
}

impl Mutex {
    /// Opens a new mutex, given the path for a folder in which the mutes will be mounted. 
    /// This will create a new floder if one does not exist yet.
    /// 
    /// # Errors
    /// 
    /// This function fails if it fcannot create the folder which is giong to contain the
    /// mutex.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Mutex> {
        fs::create_dir_all(&path)?;
        Ok(Mutex { path: path.as_ref().to_owned() })
    }

    /// Locks this mutex, awaitng for it to unlock if it is locked.
    pub async fn lock(&self) -> io::Result<MutexGuard> {
        let file_guard = FileGuard::lock(self.path.join("lock")).await?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.path.join("contents"))?;

        Ok(MutexGuard {
            _file_guard: file_guard,
            file,
        })
    }
}

/// A guard to the the [`Mutex`] when it is locked. This structure give access to
/// the contents of the mutex.
pub struct MutexGuard {
    _file_guard: FileGuard,
    file: File,
}

impl MutexGuard {
    /// Reas all the contents of the content file into a vector.
    pub fn read(&self) -> io::Result<Vec<u8>> {
        (&self.file).seek(io::SeekFrom::Start(0))?;
        (&self.file).bytes().collect::<io::Result<Vec<_>>>()
    }

    /// Writes some data to the content file, ovewritting all the previous content.
    pub fn write<D: AsRef<[u8]>>(&self, data: D) -> io::Result<()> {
        (&self.file).seek(io::SeekFrom::Start(0))?;
        self.file.set_len(0)?;
        (&self.file).write_all(data.as_ref())?;
        (&self.file).flush()
    }

    /// Gives direct access to the underlying content file.
    pub fn file(&self) -> &File {
        &self.file
    }
}

// The drop order doesn't matter. Therefore, no `Drop` implementation.

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_mutex() {
        futures::executor::block_on(async move {
            let mutex = Mutex::open("data/mutex").unwrap();
            let guard = mutex.lock().await.unwrap();

            guard.write(b"some data").unwrap();

            drop(guard);

            let guard = mutex.lock().await.unwrap();

            assert_eq!(guard.read().unwrap(), b"some data");
        });
    }

    #[test]
    fn test_mutex_longer_data_first() {
        futures::executor::block_on(async move {
            let mutex = Mutex::open("data/mutex").unwrap();
            let guard = mutex.lock().await.unwrap();

            guard.write(b"some long data").unwrap();

            drop(guard);

            let guard = mutex.lock().await.unwrap();

            guard.write(b"some data").unwrap();

            drop(guard);

            let guard = mutex.lock().await.unwrap();

            assert_eq!(guard.read().unwrap(), b"some data");
        });
    }
}
