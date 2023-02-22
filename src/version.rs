//! Versioning to pester the user not to write to incompatible queues.
//!

use semver::{Version, VersionReq};
use std::io;
use std::path::Path;

use crate::mutex::Mutex;

// fn get_version_for_queue<P: AsRef<Path>>(base: P) -> io::Result<Version> {
//     let version_file_contents = read_to_string(base.as_ref().join("yaque-version"))?;
//     let version_string = version_file_contents.trim();

//     match version_string.parse() {
//         Ok(version) => Ok(version),
//         Err(err) => panic!(
//             "failed to parse `{:?}` version file: {}; contents were `{}`",
//             base.as_ref().join("yaque-version"),
//             err,
//             version_file_contents
//         ),
//     }
// }

// /// Panics if the queue is not compatible with the current version.
// pub fn panic_if_queue_incompatible<P: AsRef<Path>>(base: P) -> io::Result<()> {
//     let version = get_version_for_queue(base.as_ref())?;
//     let requirement = VersionReq::parse(&format!(
//         "{}.{}.*",
//         env!("CARGO_PKG_VERSION_MAJOR"),
//         env!("CARGO_PKG_VERSION_MINOR")
//     ))
//     .expect("requirement is valid");

//     if !requirement.matches(&version) {
//         panic!(
//             "queue `{:?}` is of version {}, but you have yaque version {}, which is compatible with {}",
//             base.as_ref(),
//             version,
//             env!("CARGO_PKG_VERSION"),
//             requirement
//         );
//     } else {
//         Ok(())
//     }
// }

// /// Sets the version of the queue, if one doesn't exist yet.
// pub fn set_version_for_queue<P: AsRef<Path>>(base: P) -> io::Result<()> {
//     let version_file = OpenOptions::new()
//         .create_new(true)
//         .write(true)
//         .open(base.as_ref().join("yaque-version"));
//     match version_file {
//         Ok(mut file) => {
//             writeln!(file, "queue `{:?}` version is {}", base.as_ref(), env!("CARGO_PKG_VERSION"))
//         }
//         Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(()),
//         Err(err) => Err(err),
//     }
// }

/// Gets the version of the queue, or sets it if there is not one and then checks if the version is
/// compatible with the current loaded version of `yaque`. It uses a mutex to implement atomicity
/// (yes, we have had some race conditions during testing), but, for the sake of API compatibility
/// in [`crate::Sender::open`] and [`crate::Receiver::open`], it performs a spinlock, instead of 
/// `.await`ing.
pub fn check_queue_version<P: AsRef<Path>>(base: P) -> io::Result<()> {
    let mutex = Mutex::open(base.as_ref().join("version"))?;

    // Spin lock but it should be fine...
    let lock = loop {
        if let Some(lock) = mutex.try_lock()? {
            break lock;
        } else {
            std::thread::yield_now();
        }
    };

    let contents = lock.read()?;
    let str_contents = String::from_utf8_lossy(&contents);

    if str_contents.is_empty() {
        lock.write(format!("{}\n", env!("CARGO_PKG_VERSION")).as_bytes())?;
    } else {
        let version = match str_contents.trim().parse::<Version>() {
            Ok(version) => version,
            Err(err) => panic!(
                "failed to parse `{:?}` version file: {}; contents were `{}`",
                base.as_ref().join("yaque-version"),
                err,
                str_contents
            ),
        };

        let requirement = VersionReq::parse(&format!(
            "{}.{}.*",
            env!("CARGO_PKG_VERSION_MAJOR"),
            env!("CARGO_PKG_VERSION_MINOR")
        ))
        .expect("requirement is valid");

        if !requirement.matches(&version) {
            panic!(
                "queue `{:?}` is of version {}, but you have yaque version {}, which is compatible with {}",
                base.as_ref(),
                version,
                env!("CARGO_PKG_VERSION"),
                requirement
            );
        }
    }

    Ok(())
}
