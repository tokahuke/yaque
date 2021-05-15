//! # Yaque: Yet Another QUEue
//!
//! Yaque is yet another disk-backed persistent queue (and mutex) for Rust. It
//! implements an SPSC channel using your OS' filesystem. Its main advantages
//! over a simple `VecDeque<T>` are that
//! * You are not constrained by your RAM size, just by your disk size. This
//! means you can store gigabytes of data without getting OOM killed.
//! * Your data is safe even if you program panics. All the queue state is
//! written to the disk when the queue is dropped.
//! * Your data can *persist*, that is, can exist through multiple executions
//! of your program. Think of it as a very rudimentary kind of database.
//! * You can pass data between two processes.
//!
//! Yaque is _asynchronous_ and built directly on top of `mio` and `notify`.
//! It is therefore completely agnostic to the runtime you are using for you
//! application. It will work smoothly with `tokio`, with `async-std` or any
//! other executor of your choice.
//!
//! ## Sample usage
//!
//! To create a new queue, just use the [`channel`] function, passing a
//! directory path on which to mount the queue. If the directory does not exist
//! on creation, it (and possibly all its parent directories) will be created.
//! ```rust
//! use yaque::channel;
//!
//! futures::executor::block_on(async {
//!     let (mut sender, mut receiver) = channel("data/my-queue").unwrap();
//! })
//! ```
//! You can also use [`Sender::open`] and [`Receiver::open`] to open only one
//! half of the channel, if you need to. You can also use [`SenderBuilder`]
//! to instantiate a customized sender.
//!
//! The usage is similar to the MPSC channel in the standard library, except
//! that the sending and receiving methods, [`Sender::send`] and [`Receiver::recv`]
//!  are asynchronous. Writing to the queue with the sender is mostly lock-free,
//! but might lock if you limit queue size.
//! ```rust
//! use yaque::{channel, queue::try_clear};
//!
//! futures::executor::block_on(async {
//!     // Open using the `channel` function or directly with the constructors.
//!     let (mut sender, mut receiver) = channel("data/my-queue").unwrap();
//!     
//!     // Send stuff with the sender...
//!     sender.send(b"some data").await.unwrap();
//!
//!     // ... and receive it in the other side.
//!     let data = receiver.recv().await.unwrap();
//!
//!     assert_eq!(&*data, b"some data");
//!
//!     // Call this to make the changes to the queue permanent.
//!     // Not calling it will revert the state of the queue.
//!     data.commit();
//! });
//!
//! // After everything is said and done, you may delete the queue.
//! // Use `clear` for awaiting for the queue to be released.
//! try_clear("data/my-queue").unwrap();
//! ```
//! The returned value `data` is a kind of guard that implements `Deref` and
//! `DerefMut` on the underlying type.
//!
//! ## [`queue::RecvGuard`] and transactional behavior
//!
//! One important thing to notice is that reads from the queue are
//! _transactional_. The [`Receiver::recv`] returns a [`queue::RecvGuard`] that acts as
//! a _dead man switch_. If dropped, it will revert the dequeue operation,
//! unless [`queue::RecvGuard::commit`] is explicitly called. This ensures that
//! the operation reverts on panics and early returns from errors (such as when
//! using the `?` notation). However, it is necessary to perform one more
//! filesystem operation while rolling back. During drop, this is done on a
//! "best effort" basis: if an error occurs, it is logged and ignored. This is done
//! because errors cannot propagate outside a drop and panics in drops risk the
//! program being aborted. If you _have_ any cleanup behavior for an error from
//! rolling back, you may call [`queue::RecvGuard::rollback`] which _will_ return the
//! underlying error.
//!
//! ## Batches
//!
//! You can use the `yaque` queue to send and receive batches of data ,
//! too. The guarantees are the same as with single reads and writes, except
//! that you may save on OS overhead when you send items, since only one disk
//! operation is made. See [`Sender::send_batch`], [`Receiver::recv_batch`] and
//! [`Receiver::recv_until`] for more information on receiver batches.
//!
//! ## Tired of `.await`ing? Timeouts are supported
//!
//! If you need your application to not stall when nothing is being put on the
//! queue, you can use [`Receiver::recv_timeout`] and
//! [`Receiver::recv_batch_timeout`] to receive data, awaiting up to a
//! completion of a provided future, such as a delay or a channel. Here is an
//! example:
//! ```rust
//! use yaque::channel;
//! use std::time::Duration;
//! use futures_timer::Delay;
//!
//! futures::executor::block_on(async {
//!     let (mut sender, mut receiver) = channel("data/my-queue-2").unwrap();
//!     
//!     // receive some data up to a second
//!     let data = receiver
//!         .recv_timeout(Delay::new(Duration::from_secs(1)))
//!         .await
//!         .unwrap();
//!
//!     // Nothing was sent, so no data...
//!     assert!(data.is_none());
//!     drop(data);
//!     
//!     // ... but if you do send something...
//!     sender.send(b"some data").await.unwrap();
//!  
//!     // ... now you receive something:
//!     let data = receiver
//!         .recv_timeout(Delay::new(Duration::from_secs(1)))
//!         .await
//!         .unwrap();
//!
//!     assert_eq!(&*data.unwrap(), b"some data");  
//! });
//! ```
//!
//! ## `Ctrl+C` and other unexpected events
//!
//! First of all, "Don't panic©"! Writing to the queue is an atomic operation.
//! Therefore, unless there is something really wrong with your OS, you should be
//! fine in terms of data corruption most of the time.
//!
//! In case of a panic (the program's, not the programmer's), the queue is
//! guaranteed to save all the most up-to-date metadata for the receiver. For
//! the reader it is even simpler: there is nothing to be saved in the first
//! place. The only exception to this guarantee is if the saving operation fails
//! due to an IO error. Remember that the program is not allowed to panic during
//! a panic. Therefore in this case, `yaque` will not attempt to recover from an
//! error.
//!
//! The same thing cannot be said from OS signals. Signals from the OS are *not*
//! handled automatically by this library. It is understood that the application
//! programmer knows best how to handle them. If you chose to close queue on
//! `Ctrl+C` or other signals, you are in luck! Saving both sides of the queue
//! is [async-signal-safe](https://man7.org/linux/man-pages/man7/signal-safety.7.html)
//! so you may set up a bare signal hook directly using, for example,
//! [`signal_hook`](https://docs.rs/signal-hook/), if you are the sort of person
//! that enjoys `unsafe` code. If not, there are a ton of completely safe
//! alternatives out there. Choose the one that suits you the best.
//!
//! Unfortunately, there are also times when you get `aborted` or `killed`. These
//! signals cannot be handled by any library whatsoever. When this happens, not
//! everything is lost yet. We provied a whole module, [`recovery`],
//! to aid you in automatic queue recovery. Please check the module for the
//! specific function names. From an architectural perspective, we offer two
//! different approaches to queue recovery, which may be suitable to different
//! use cases:
//!
//! 1. Recover with replay (the standard): we can reconstruct a _lower bound_
//! of the actual state of the queue during the crash, which consists of the
//! _maximum_ of the following two positions:
//!     * the bottom of the smallest segment still present in the directory.
//!     * the position indicated in the metadata file.
//!
//! Since this is a lower bound, some elements may be replayed. If your
//! processing is _idempotent_, this will not be an issue and you lose no data
//! whatsoever.
//!
//! 2. Recover with loss: we can also reconstruct an _upper bound_ for the
//! actual state of the queue: the bottom of the second smallest segment in
//! the queue. In this case, the smallest segment is simply erased and the
//! receiver caries on as if nothing has happened. If replays are intollerable,
//! but some data loss is, this might be the right alternative for you. You can
//! limit data loss by constraining the segment size, configuring this option on
//! [`SenderBuilder`].
//!
//! If you really want to err on the safer side, you may use [`Receiver::save`]
//! to periodically back the receiver state up. Just choose you favorite timer
//! implementation and set a simple periodical task up every hundreds of milliseconds.
//! However, be warned that this is only a _mitigation_ of consistency problems, not
//! a solution.
//!
//! ## Known issues and next steps
//!
//! * ~~This is a brand new project. Although I have tested it and it will
//! certainly not implode your computer, don't trust your life on it yet.~~ This code
//! is running in production for non-critical applications.
//! * Wastes too much kernel time when the queue is small enough and the sender
//! sends many frequent small messages non-atomically. You can mitigate that by
//! writing in batches to the queue.
//! * There are probably unknown bugs hidden in some corner case. If you find
//! one, please [fill an issue on GitHub](https://github.com/tokahuke/yaque/issues/new).
//! Pull requests and contributions are also greatly appreciated.
//!

mod error;
mod header;
mod state;
mod sync;
mod version;
mod watcher;

pub mod mutex;
pub mod queue;
#[cfg(feature = "recovery")]
pub mod recovery;

pub use error::{TryRecvError, TrySendError};
pub use queue::{channel, Receiver, ReceiverBuilder, Sender, SenderBuilder, QueueIter};
