use std::fs::*;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// A lock using the atomicity of `OpenOptions::create_new`. Not exactly a good
/// lock. You can easly delete it and everything goes down the drain.
pub struct FileGuard {
    path: PathBuf,
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        if let Err(err) = remove_file(&self.path) {
            log::error!("CRITICAL: unable to drop file lock: {}", err);
        }
    }
}

impl FileGuard {
    /// Tries to lock using a certain path in the disk. If the file exists,
    /// returns `Ok(None)`.
    pub fn try_lock<P: AsRef<Path>>(path: P) -> io::Result<Option<FileGuard>> {
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                writeln!(file, "pid={}", std::process::id())?;
                Ok(Some(FileGuard {
                    path: path.as_ref().to_path_buf(),
                }))
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(None),
            Err(err) => Err(err),
        }
    }
}

/// The internal state of one side of the queue.'
#[derive(Debug, PartialEq)]
pub struct QueueState {
    pub segment_size: u64,
    pub segment: u64,
    pub position: u64,
}

impl Default for QueueState {
    fn default() -> QueueState {
        QueueState {
            segment_size: 1024 * 1024 * 32,
            segment: 0,
            position: 0,
        }
    }
}

impl QueueState {
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

    /// Test if position is past the end of the segment.
    pub fn is_past_end(&self) -> bool {
        self.position > self.segment_size
    }
}

/// An implmementation of persistence using the filesystem itself.
#[derive(Default)]
pub struct FilePersistence {
    path: Option<PathBuf>,
}

/// The name of the file from the sender side inside the queue folder.
fn send_persistence_filename<P: AsRef<Path>>(base: P) -> PathBuf {
    base.as_ref().join("send-metadata")
}

/// The name of the file inside the queue folder.
fn recv_persistence_filename<P: AsRef<Path>>(base: P) -> PathBuf {
    base.as_ref().join("recv-metadata")
}

impl FilePersistence {
    /// Creates a new file persistence.
    pub fn new() -> FilePersistence {
        FilePersistence::default()
    }

    fn open<P: AsRef<Path>>(&mut self, path: P) -> io::Result<QueueState> {
        self.path = Some(path.as_ref().to_path_buf());

        let mut u64_buffer = [0; 8];

        match File::open(&path) {
            Ok(mut file) => {
                let mut read_u64 = move || -> io::Result<_> {
                    file.read_exact(&mut u64_buffer)?;
                    Ok(u64::from_be_bytes(u64_buffer))
                };

                Ok(QueueState {
                    segment_size: read_u64()?,
                    segment: read_u64()?,
                    position: read_u64()?,
                })
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(QueueState::default()),
            Err(err) => Err(err),
        }
    }

    /// Returns the queue state for the given queue path.
    pub fn open_send<P: AsRef<Path>>(&mut self, base: P) -> io::Result<QueueState> {
        self.open(send_persistence_filename(base))
    }

    /// Returns the queue state for the given queue path. This method will
    /// always be invoked *before* and calls to `save` are made.
    pub fn open_recv<P: AsRef<Path>>(&mut self, base: P) -> io::Result<QueueState> {
        self.open(recv_persistence_filename(base))
    }

    /// Saves the queue state.
    pub fn save(&mut self, queue_state: &QueueState) -> io::Result<()> {
        let mut file = BufWriter::new(File::create(
            self.path
                .as_ref()
                .expect("save should be called *after* open"),
        )?);

        file.write_all(&queue_state.segment_size.to_be_bytes())?;
        file.write_all(&queue_state.segment.to_be_bytes())?;
        file.write_all(&queue_state.position.to_be_bytes())?;
        file.flush()?;

        Ok(())
    }
}
