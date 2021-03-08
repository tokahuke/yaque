use std::fmt;
use std::io;

/// An error that occurrs when trying to send into a queue.
#[derive(Debug)]
pub enum TrySendError<T> {
    /// An underlying IO error occurred.
    Io(io::Error),
    /// The queue has reached its maximum capacity and is not open to be sent.
    QueueFull { item: T, queue_name: String },
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
            TrySendError::QueueFull { queue_name, .. } => {
                write!(f, "the queue `{}` is full", queue_name)
            }
        }
    }
}

impl<T> TrySendError<T> {
    /// Tries to unwrap the IO error. If not able to, panics. You can use the following pattern in your code:
    /// ```
    /// queue.try_send(b"some stuff").map_err(TrySendError::unwrap_io)?;
    /// ```
    pub fn unwrap_io(self) -> io::Error {
        match self {
            TrySendError::Io(error) => error,
            TrySendError::QueueFull { queue_name, .. } => panic!(
                "was expecting TrySendError::Io; got TrySendError::QueueFull (at queue `{}`)",
                queue_name
            ),
        }
    }
}
