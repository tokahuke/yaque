//! Recovery utilities for queues left in as inconsistent state. Use these
//! functions if you need to automatically recover from a failure.
//!
//! This module is dependent on the `recovery` feature, which is enabled
//! by default.
//!
use std::fs::*;
use std::io;
use std::path::Path;
use sysinfo::*;

use super::state::{FilePersistence, QueueState};
use super::sync::UNIQUE_PROCESS_TOKEN;
use super::{recv_lock_filename, send_lock_filename, FileGuard};

/// Unlocks a lock file if the owning process does not exist anymore. This
/// function does nothing if the file does not exist.
///
/// # Panics
///
/// This function panics if it cannot parse the lockfile.
fn unlock<P: AsRef<Path>>(lock_filename: P) -> io::Result<()> {
    let contents = match read_to_string(&lock_filename) {
        Ok(contents) => contents,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };

    let owner_pid = contents
        .split("pid=")
        .collect::<Vec<_>>()
        .get(1)
        .map(|token| {
            token
                .chars()
                .take_while(|ch| ch.is_digit(10))
                .collect::<String>()
                .parse::<sysinfo::Pid>()
        })
        .expect("failed to parse recv lock file: no pid")
        .expect("failed to parse recv lock file: bad pid");

    let owner_token = contents
        .split("token=")
        .collect::<Vec<_>>()
        .get(1)
        .map(|token| {
            token
                .chars()
                .take_while(|ch| ch.is_digit(10))
                .collect::<String>()
                .parse::<u64>()
        })
        .expect("failed to parse recv lock file: no token")
        .expect("failed to parse recv lock file: bad token");

    let system = System::new_with_specifics(RefreshKind::new().with_processes());

    // Maybe somebody else is holding the lock:
    let process_exists_and_is_not_me =
        owner_pid as u32 != std::process::id() && system.get_processes().get(&owner_pid).is_some();
    // I am holding the lock:
    let lock_is_the_same_and_is_me =
        owner_pid as u32 == std::process::id() && owner_token == *UNIQUE_PROCESS_TOKEN;

    if process_exists_and_is_not_me {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "another process, of id {}, is still locking `{:?}`",
                owner_pid,
                lock_filename.as_ref()
            ),
        ));
    } else if lock_is_the_same_and_is_me {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "current process is still locking `{:?}`",
                lock_filename.as_ref()
            ),
        ));
    } else {
        remove_file(lock_filename)?;
        Ok(())
    }
}

/// Unlocks a queue in a given directory for sending. This function returns an
/// error of kind `io::ErrorKind::Other` when the process listed in the
/// lockfile still exists.
///
/// # Panics
///
/// This function panics if it cannot parse the lockfile.
pub fn unlock_for_sending<P: AsRef<Path>>(base: P) -> io::Result<()> {
    unlock(send_lock_filename(base.as_ref()))
}

/// Unlocks a queue in a given directory for receiving. This function returns
/// an error of kind `io::ErrorKind::Other` when the process listed in the
/// lockfile still exists.
///
/// # Panics
///
/// This function panics if it cannot parse the lockfile.
pub fn unlock_for_receiving<P: AsRef<Path>>(base: P) -> io::Result<()> {
    unlock(recv_lock_filename(base.as_ref()))
}

/// Unlocks a queue in a given directory for both sending and receiving. This
/// function is the combination of [`unlock_for_sending`] and
/// [`unlock_for_receiving`].
///
/// # Panics
///
/// This function panics if it cannot parse either of the lock files.
pub fn unlock_queue<P: AsRef<Path>>(base: P) -> io::Result<()> {
    unlock_for_sending(base.as_ref())?;
    unlock_for_receiving(base.as_ref())?;

    Ok(())
}

/// Guesses the send metadata for a given queue. This equals to the top
/// position in the greatest segment present in the directory. This function
/// will substitute the current send metadata by this guess upon acquiring
/// the send lock on this queue.
///
/// # Panics
///
/// This function panics if there is a file in the queue folder with extension
/// `.q` whose name is not an integer, such as `foo.q`.
pub fn guess_send_metadata<P: AsRef<Path>>(base: P) -> io::Result<()> {
    // Lock for sending:
    let lock = FileGuard::try_lock(send_lock_filename(base.as_ref()))?;

    // Find greatest segment:
    let mut max_segment = 0;
    for maybe_entry in read_dir(base.as_ref())? {
        let path = maybe_entry?.path();
        if path.extension().map(|ext| ext == "q").unwrap_or(false) {
            let segment = path
                .file_stem()
                .expect("has extension, therefore has stem")
                .to_string_lossy()
                .parse::<u64>()
                .expect("failed to parse segment filename");

            max_segment = u64::max(segment, max_segment);
        }
    }

    // Find top position in the segment:
    let segment_metadata = metadata(base.as_ref().join(format!("{}.q", max_segment)))?;
    let position = segment_metadata.len();

    // Generate new queue state:
    let queue_state = QueueState {
        segment: max_segment,
        position,
        ..QueueState::default()
    };

    // And save:
    let mut persistence = FilePersistence::new();
    let _ = persistence.open_send(base.as_ref())?;
    persistence.save(&queue_state)?;

    // Drop lock for sending:
    drop(lock);

    Ok(())
}

/// Recovers a queue, appliying the following operations, in this order:
/// * Unlocks both the sender and receiver side of the queue.
/// * Guesses the position of the sender using [`guess_send_metadata`].
///
/// # Panics
///
/// This function panics if there is a file in the queue folder with extension
/// `.q` whose name is not an integer, such as `foo.q` or if the lockfiles for
/// either sending or receiving cannot be parsed.
pub fn recover<P: AsRef<Path>>(base: P) -> io::Result<()> {
    unlock_queue(base.as_ref())?;
    guess_send_metadata(base.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unlock() {
        // Create a guard:
        let guard = FileGuard::try_lock("data/test-unlock.lock").unwrap();

        // "Forget" to drop it:
        std::mem::forget(guard);

        // Now, try to unlock:
        assert_eq!(
            io::ErrorKind::Other,
            unlock("data/test-unlock.lock").unwrap_err().kind()
        );

        // Clean up:
        remove_file("data/test-unlock.lock").unwrap();
    }

    #[test]
    fn test_unlock_inexistent() {
        unlock("data/inexistent-lock.lock").unwrap();
    }

    // TODO missing test for `guess_send_metadata` and `recover`.

    #[test]
    fn test_guess_send_metadata() {
        unimplemented!()
    }

    #[test]
    fn test_recover() {
        unimplemented!()
    }
}
