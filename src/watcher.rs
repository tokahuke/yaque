use notify::event::{Event, EventKind, ModifyKind};
use notify::{RecommendedWatcher, Watcher};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::task::Waker;

// /// Watches for the creation of a given future file.
// fn file_creation_watcher<P>(path: P, waker: Arc<Mutex<Option<Waker>>>) -> RecommendedWatcher
// where
//     P: AsRef<Path>,
// {
//     // Set up watcher:
//     let mut watcher =
//         notify::immediate_watcher(move |maybe_event: notify::Result<notify::Event>| {
//             match maybe_event.expect("received error from watcher") {
//                 // When any modification in the file happens
//                 Event {
//                     kind: EventKind::Create(_),
//                     ..
//                 } => {
//                     waker
//                         .lock()
//                         .expect("waker poisoned")
//                         .as_mut()
//                         .map(|waker: &mut Waker| waker.wake_by_ref());
//                 }
//                 _ => {}
//             }
//         })
//         .expect("could not create watcher");

//     // Put watcher to run:
//     watcher
//         .watch(
//             path.as_ref().parent().expect("file must have parent"),
//             notify::RecursiveMode::NonRecursive,
//         )
//         .expect("could not start watching file");

//     watcher
// }

/// Watches for the removal of a given future file.
pub(crate) fn file_removal_watcher<P>(
    path: P,
    waker: Arc<Mutex<Option<Waker>>>,
) -> RecommendedWatcher
where
    P: AsRef<Path>,
{
    // Set up watcher:
    let mut watcher =
        notify::immediate_watcher(move |maybe_event: notify::Result<notify::Event>| {
            match maybe_event.expect("received error from watcher") {
                // When any modificatin in the file happens
                Event {
                    kind: EventKind::Remove(_),
                    ..
                } => {
                    waker
                        .lock()
                        .expect("waker poisoned")
                        .as_mut()
                        .map(|waker: &mut Waker| waker.wake_by_ref());
                }
                _ => {}
            }
        })
        .expect("could not create watcher");

    // Put watcher to run:
    watcher
        .watch(
            path.as_ref().parent().expect("file must have parent"),
            notify::RecursiveMode::NonRecursive,
        )
        .expect("could not start watching file");

    watcher
}

/// Watches a file for changes in its content.
pub(crate) fn file_watcher<P>(path: P, waker: Arc<Mutex<Option<Waker>>>) -> RecommendedWatcher
where
    P: AsRef<Path>,
{
    // Set up watcher:
    let mut watcher =
        notify::immediate_watcher(move |maybe_event: notify::Result<notify::Event>| {
            match maybe_event.expect("received error from watcher") {
                // When any modification in the file happens
                Event {
                    kind: EventKind::Modify(ModifyKind::Data(_)),
                    ..
                } => {
                    waker
                        .lock()
                        .expect("waker poisoned")
                        .as_mut()
                        .map(|waker: &mut Waker| waker.wake_by_ref());
                }
                Event {
                    kind: EventKind::Remove(_),
                    ..
                } => {
                    log::debug!("file being watched was removed");
                }
                _ => {}
            }
        })
        .expect("could not create watcher");

    // Put watcher to run:
    watcher
        .watch(path, notify::RecursiveMode::NonRecursive)
        .expect("could not start watching file");

    watcher
}
