//! Recovery utilities for queues left in as inconsistent state, based on "best
//! effort" strategies. Use these functions if you need to automatically recover
//! from a failure.
//!
use std::fs::*;
use std::io;
use std::path::Path;
use sysinfo::*;

use super::queue::{recv_lock_filename, send_lock_filename};
use super::state::{QueueState, QueueStatePersistence};
use super::sync::{FileGuard, UNIQUE_PROCESS_TOKEN};

/// Unlocks a lock file if the owning process does not exist anymore. This
/// function does nothing if the file does not exist.
///
/// # Panics
///
/// This function panics if it cannot parse the lockfile.
pub fn unlock<P: AsRef<Path>>(lock_filename: P) -> io::Result<()> {
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

/// Guesses the receive metadata for a given queue. This equals to the bottom
/// position in the smallest segment present in the directory or the existing
/// receiver metadata, whichever is greater. The reason for this is that the
/// receive metadata is a lower bound of where the receiver actually was and this
/// guess is always lower than that.
///
/// It is important to note two things:
/// 1. The data in the current segment will be lost.
/// 2. You don't need to use this function when replays are acceptable, since
/// the existing metadata file is already a lower bound of the actual state of
/// the queue.
///
/// You should *not* use this function if you suppose that your data was corrupted.
///
/// # Panics
///
/// This function panics if there is a file in the queue folder with extension
/// `.q` whose name is not an integer, such as `foo.q`.
pub fn guess_recv_metadata<P: AsRef<Path>>(base: P) -> io::Result<()> {
    // Lock for receiving:
    let lock = FileGuard::try_lock(recv_lock_filename(base.as_ref()))?;

    // Find smallest segment:
    let mut min_segment = std::u64::MAX;
    for maybe_entry in read_dir(base.as_ref())? {
        let path = maybe_entry?.path();
        if path.extension().map(|ext| ext == "q").unwrap_or(false) {
            let segment = path
                .file_stem()
                .expect("has extension, therefore has stem")
                .to_string_lossy()
                .parse::<u64>()
                .expect("failed to parse segment filename");

            min_segment = u64::min(segment, min_segment);
        }
    }

    // Generate new queue state:
    let queue_state = QueueState {
        segment: min_segment,
        ..QueueState::default()
    };

    // And save the max between the old state and the guessed state:
    let mut persistence = QueueStatePersistence::new();
    let old_state = persistence.open(base.as_ref())?;
    persistence.save(if queue_state > old_state {
        &queue_state
    } else {
        &old_state
    })?;

    // Drop lock for receiving:
    drop(lock);

    Ok(())
}

/// Guesses the receive metadata for a given queue. This equals to the bottom
/// position in the segment after the smallest one present in the directory.
/// This function will substitute the current receive metadata by this guess upon
/// acquiring the receive lock on this queue.
///
/// It is important to note two things:
/// 1. The data in the current segment will be lost.
/// 2. You don't need to use this function when replays are acceptable, since
/// the existing metadata file is already a lower bound of the actual state of
/// the queue.
///
/// You should use this function if you suppose that your data was corrupted.
///
/// # Panics
///
/// This function panics if there is a file in the queue folder with extension
/// `.q` whose name is not an integer, such as `foo.q`.
pub fn guess_recv_metadata_with_loss<P: AsRef<Path>>(base: P) -> io::Result<()> {
    // Lock for receiving:
    let lock = FileGuard::try_lock(recv_lock_filename(base.as_ref()))?;

    // Find smallest segment:
    let mut min_segment = std::u64::MAX;
    for maybe_entry in read_dir(base.as_ref())? {
        let path = maybe_entry?.path();
        if path.extension().map(|ext| ext == "q").unwrap_or(false) {
            let segment = path
                .file_stem()
                .expect("has extension, therefore has stem")
                .to_string_lossy()
                .parse::<u64>()
                .expect("failed to parse segment filename");

            min_segment = u64::min(segment, min_segment);
        }
    }

    // Destroy the current segment:
    remove_file(base.as_ref().join(format!("{}.q", min_segment)))?;

    // Generate new queue state:
    let queue_state = QueueState {
        // as soon as the receiver is initialized, it will go to the next block because it was
        // already at the end of a block.
        segment: min_segment + 1,
        ..QueueState::default()
    };

    // And save:
    let mut persistence = QueueStatePersistence::new();
    let _ = persistence.open(base.as_ref())?;
    persistence.save(&queue_state)?;

    // Drop lock for receiving:
    drop(lock);

    Ok(())
}

/// Recovers a queue, appliying the following operations, in this order:
/// * Unlocks both the sender and receiver side of the queue.
/// * Guesses the position of the receiver using [`guess_recv_metadata`] (this
/// is just the existing state of the receiver most of the time).
///
/// This means that some of the data may be replayed, since the receiver
/// metadata is rarely touched (and it is *always* a lower bound of where the
/// receiver actually was). If replays are not acceptable, see
/// [`recover_with_loss`].
///
/// You should also use [`recover_with_loss`] if you suppose your data was
/// corrupted.
///  
/// # Panics
///
/// This function panics if there is a file in the queue folder with extension
/// `.q` whose name is not an integer, such as `foo.q` or if the lockfiles for
/// either sending or receiving cannot be parsed.
pub fn recover<P: AsRef<Path>>(base: P) -> io::Result<()> {
    unlock_queue(base.as_ref())?;
    guess_recv_metadata(base.as_ref())?;

    Ok(())
}

/// Recovers a queue, appliying the following operations, in this order:
/// * Unlocks both the sender and receiver side of the queue.
/// * Guesses the position of the receiver using [`guess_recv_metadata_with_loss`]
/// (this truncates the bottom segment of the queue, resulting in data loss).
///
/// This means that some of the data may be lost, since the bottom segment is
/// erased. If data loss is not acceptable, see [`recover`].
///
/// You should not use [`recover`] if you suppose your data was corrupted.
///  
/// # Panics
///
/// This function panics if there is a file in the queue folder with extension
/// `.q` whose name is not an integer, such as `foo.q` or if the lockfiles for
/// either sending or receiving cannot be parsed.
pub fn recover_with_loss<P: AsRef<Path>>(base: P) -> io::Result<()> {
    unlock_queue(base.as_ref())?;
    // this has to be first because it messes with the directory structure, invalidating a possible
    // call to `guess_send_metadata`.
    guess_recv_metadata_with_loss(base.as_ref())?;

    Ok(())
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

    // #[test]
    // fn test_guess_send_metadata() {
    //     unimplemented!()
    // }

    #[test]
    #[should_panic]
    fn test_recover_while_open() {
        // Create a channel:
        let channel = crate::channel("data/recover-while-open").unwrap();

        recover("data/recover-while-open").unwrap();

        drop(channel);
    }

    #[test]
    #[should_panic]
    fn test_recover_inexistent() {
        recover("data/recover-inexistent").unwrap();
    }
}
