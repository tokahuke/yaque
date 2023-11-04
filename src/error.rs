//! Errors specific to `yaque`.

use std::fmt;
use std::io;
use std::path::PathBuf;

/// An error that occurrs when trying to send into a queue.
#[derive(Debug)]
pub enum TrySendError<T> {
    /// An underlying IO error occurred.
    Io(io::Error),
    /// The queue has reached its maximum capacity and is not open to be sent.
    QueueFull {
        /// The thing that we were trying to send.
        item: T,
        /// The path for the queue where this problem happened.
        base: PathBuf,
    },
}

impl<T> From<io::Error> for TrySendError<T> {
    fn from(error: io::Error) -> TrySendError<T> {
        TrySendError::Io(error)
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Io(error) => write!(f, "io error: {}", error),
            TrySendError::QueueFull { base, .. } => {
                write!(f, "the queue `{:?}` is full", base)
            }
        }
    }
}

impl<T> TrySendError<T> {
    /// Tries to unwrap the IO error. If not able to, panics. You can use the following pattern in your code:
    /// ```ignore
    /// queue.try_send(b"some stuff").map_err(TrySendError::unwrap_io)?;
    /// ```
    pub fn unwrap_io(self) -> io::Error {
        match self {
            TrySendError::Io(error) => error,
            TrySendError::QueueFull { base, .. } => panic!(
                "was expecting TrySendError::Io; got TrySendError::QueueFull at queue `{:?}`",
                base
            ),
        }
    }
}

/// An error that occurs when trying to receive from an empty queue.
#[derive(Debug)]
pub enum TryRecvError {
    /// An underlying IO error occurred.
    Io(io::Error),
    /// The queue is empty and there is nothing to be received right now.
    QueueEmpty, // { base: PathBuf }, problems with borrow checker. Leave it for future release...
}

impl From<io::Error> for TryRecvError {
    fn from(error: io::Error) -> TryRecvError {
        TryRecvError::Io(error)
    }
}

impl TryRecvError {
    /// Tries to unwrap the IO error. If not able to, panics. You can use the following pattern in your code:
    /// ```ignore
    /// queue.try_send(b"some stuff").map_err(TryRecvError::unwrap_io)?;
    /// ```
    pub fn unwrap_io(self) -> io::Error {
        match self {
            TryRecvError::Io(error) => error,
            TryRecvError::QueueEmpty => {
                panic!("was expecting TryRecvError::Io; got TryRecvError::QueueEmpty",)
            }
        }
    }

    pub(crate) fn result_from_option<T>(option: Option<io::Result<T>>) -> Result<T, TryRecvError> {
        match option {
            Some(Ok(t)) => Ok(t),
            Some(Err(err)) => Err(TryRecvError::Io(err)),
            None => Err(TryRecvError::QueueEmpty),
        }
    }
}
