//! Structures for managing the state of a queue.

use std::cmp::{Ordering, PartialOrd};
use std::fs::*;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// The internal state of one side of the queue.
#[derive(Debug, PartialEq)]
pub struct QueueState {
    /// The number of the actual segment.
    pub segment: u64,
    /// The byte position within the segment (the position that can be reached
    /// by using Seek::seek).
    pub position: u64,
}

impl Default for QueueState {
    fn default() -> QueueState {
        QueueState {
            segment: 0,
            position: 0,
        }
    }
}

impl PartialOrd for QueueState {
    fn partial_cmp(&self, other: &QueueState) -> Option<Ordering> {
        if self.segment > other.segment {
            Some(Ordering::Greater)
        } else if self.segment < other.segment {
            Some(Ordering::Less)
        } else {
            Some(self.position.cmp(&other.position))
        }
    }
}

impl QueueState {
    /// Guesses the send metadata for a given queue. This equals to the top
    /// position in the greatest segment present in the directory. This function
    /// will substitute the current send metadata by this guess upon acquiring
    /// the send lock on this queue.
    ///
    /// # Panics
    ///
    /// This function panics if there is a file in the queue folder with extension
    /// `.q` whose name is not an integer, such as `foo.q`.
    pub fn for_send_metadata<P: AsRef<Path>>(base: P) -> io::Result<QueueState> {
        match read_dir(base.as_ref()) {
            Ok(read_dir) => {
                log::trace!("base {:?} exists", base.as_ref());

                // Find greatest segment:
                let mut max_segment = None;
                for maybe_entry in read_dir {
                    let path = maybe_entry?.path();
                    if path.extension().map(|ext| ext == "q").unwrap_or(false) {
                        let segment = path
                            .file_stem()
                            .expect("has extension, therefore has stem")
                            .to_string_lossy()
                            .parse::<u64>()
                            .expect("failed to parse segment filename");

                        max_segment = if let Some(max_segment) = max_segment {
                            Some(u64::max(segment, max_segment))
                        } else {
                            Some(segment)
                        };
                    }
                }

                // May not have found any segment. In this case, queue is empty.
                if let Some(max_segment) = max_segment {
                    // Find top position in the segment:
                    let segment_metadata =
                        metadata(base.as_ref().join(format!("{}.q", max_segment)))?;
                    let position = segment_metadata.len();

                    // Generate new queue state:
                    let queue_state = QueueState {
                        segment: max_segment,
                        position,
                        ..QueueState::default()
                    };

                    Ok(queue_state)
                } else {
                    log::trace!("queue {:?} was empty", base.as_ref());
                    Ok(QueueState::default())
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                log::trace!("base {:?} doesn't exist", base.as_ref());
                Ok(QueueState::default())
            }
            Err(err) => Err(err),
        }
    }

    /// Advances to the next segment.
    pub fn advance_segment(&mut self) -> u64 {
        self.position = 0;
        self.segment += 1;
        self.segment
    }

    /// Goes back to the last segment.
    pub fn retreat_segment(&mut self) {
        self.segment -= 1;
    }

    /// Advances the position in the segment.'
    pub fn advance_position(&mut self, offset: u64) {
        self.position += offset;
    }
}

/// An implementation of persistence using the filesystem itself.
#[derive(Default)]
pub struct QueueStatePersistence {
    path: Option<PathBuf>,
}

/// The name of the file inside the queue folder.
fn recv_persistence_filename<P: AsRef<Path>>(base: P) -> PathBuf {
    base.as_ref().join("recv-metadata")
}

impl QueueStatePersistence {
    /// Creates a new file persistence.
    pub fn new() -> QueueStatePersistence {
        QueueStatePersistence::default()
    }

    pub fn open<P: AsRef<Path>>(&mut self, base: P) -> io::Result<QueueState> {
        let path = recv_persistence_filename(base);
        self.path = Some(path.clone());

        let mut u64_buffer = [0; 8];

        match File::open(&path) {
            Ok(mut file) => {
                let mut read_u64 = move || -> io::Result<_> {
                    file.read_exact(&mut u64_buffer)?;
                    Ok(u64::from_be_bytes(u64_buffer))
                };

                Ok(QueueState {
                    segment: read_u64()?,
                    position: read_u64()?,
                })
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(QueueState::default()),
            Err(err) => Err(err),
        }
    }

    /// Saves the queue state.
    pub fn save(&mut self, queue_state: &QueueState) -> io::Result<()> {
        let mut file = BufWriter::new(File::create(
            self.path
                .as_ref()
                .expect("save should be called *after* open"),
        )?);

        file.write_all(&queue_state.segment.to_be_bytes())?;
        file.write_all(&queue_state.position.to_be_bytes())?;
        file.flush()?;

        Ok(())
    }
}
