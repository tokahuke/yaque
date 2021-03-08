use std::io;
use std::fmt;

#[derive(Debug)]
pub enum TrySendError {
    /// An underlying IO error occurred.
    Io(io::Error),
    /// The queue has reached its maimum capacity and is not open to be sent.
    QueueFull { queue_name: String },
}

impl From<io::Error> for TrySendError {
    fn from(error: io::Error) -> TrySendError {
        TrySendError::Io(error)
    }
}

impl fmt::Display for TrySendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Io(error) => write!(f, "io error: {}", error),
            TrySendError::QueueFull { queue_name } => write!(f, "the queue `{}` is full", queue_name),
        }
    }
}
