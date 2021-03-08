use std::io;
use std::fmt;

#[derive(Debug)]
pub enum TrySendError<T> {
    /// An underlying IO error occurred.
    Io(io::Error),
    /// The queue has reached its maimum capacity and is not open to be sent.
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
            TrySendError::QueueFull { queue_name, .. } => write!(f, "the queue `{}` is full", queue_name),
        }
    }
}
