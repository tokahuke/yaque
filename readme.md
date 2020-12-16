# Yaque: Yet Another QUEue

Yaque is yet another disk-backed persistent queue for Rust. It implements
an SPSC channel using your OS' filesystem. Its main advantages over a simple
`VecDeque<T>` are that
* You are not constrained by your RAM size, just by your disk size. This
means you can store gigabytes of data without getting OOM killed.
* Your data is safe even if you program panics. All the queue state is
written to the disk when the queue is dropped.
* Your data can *persist*, that is, can exist through multiple executions
of your program. Think of it as a very rudimentary kind of database.
* You can pass data between two processes.

Yaque is _asynchronous_ and built directly on top of `mio` and `notify`.
It is therefore completely agnostic to the runtime you are using for you
application. It will work smoothly with `tokio`, with `async-std` or any
other executor of your choice.

## Sample usage

To create a new queue, just use the `channel` function, passing a
directory path on which to mount the queue. If the directory does not exist
on creation, it (and possibly all its parent directories) will be created.
```rust
use yaque::channel;

futures::executor::block_on(async {
    let (mut sender, mut receiver) = channel("data/my-queue").unwrap();
})
```
You can also use `Sender::open` and `Receiver::open` to open only one
half of the channel, if you need to.

The usage is similar to the MPSC channel in the standard library, except
that the receiving method, `Receiver::recv` is asynchronous. Writing to
the queue with the sender is basically lock-free and atomic.
```rust
use yaque::{channel, try_clear};

futures::executor::block_on(async {
    // Open using the `channel` function or directly with the constructors.
    let (mut sender, mut receiver) = channel("data/my-queue").unwrap();
    
    // Send stuff with the sender...
    sender.send(b"some data").unwrap();

    // ... and receive it in the other side.
    let data = receiver.recv().await.unwrap();

    assert_eq!(&*data, b"some data");

    // Call this to make the changes to the queue permanent.
    // Not calling it will revert the state of the queue.
    data.commit();
});

// After everything is said and done, you may delete the queue.
// Use `clear` for awaiting for the queue to be released.
try_clear("data/my-queue").unwrap();
```
The returned value `data` is a kind of guard that implements `Deref` and
`DerefMut` on the underlying type.

## `queue::RecvGuard` and transactional behavior

One important thing to notice is that reads from the queue are
_transactional_. The `Receiver::recv` returns a `queue::RecvGuard` that acts as
a _dead man switch_. If dropped, it will revert the dequeue operation,
unless `queue::RecvGuard::commit` is explicitly called. This ensures that
the operation reverts on panics and early returns from errors (such as when
using the `?` notation). However, it is necessary to perform one more
filesystem operation while rolling back. During drop, this is done on a
"best effort" basis: if an error occurs, it is logged and ignored. This is done
because errors cannot propagate outside a drop and panics in drops risk the
program being aborted. If you _have_ any cleanup behavior for an error from
rolling back, you may call `queue::RecvGuard::rollback` which _will_ return the
underlying error.

## Batches

You can use the `yaque` queue to send and receive batches of data ,
too. The guarantees are the same as with single reads and writes, except
that you may save on OS overhead when you send items, since only one disk
operation is made. See `Sender::send_batch`, `Receiver::recv_batch` and
`Receiver::recv_until` for more information on receiver batches.

## Tired of `.await`ing? Timeouts are supported

If you need your application to not stall when nothing is being put on the
queue, you can use `Receiver::recv_timeout` and 
`Receiver::recv_batch_timeout` to receive data, awaiting up to a 
completion of a provided future, such as a delay or a channel. Here is an 
example:
```rust
use yaque::{channel, try_clear};
use std::time::Duration;
use futures_timer::Delay;

futures::executor::block_on(async {
    let (mut sender, mut receiver) = channel("data/my-queue").unwrap();
    
    // receive some data up to a second
    let data = receiver.recv_timeout(Delay::new(Duration::from_secs(1))).await.unwrap();

    // Nothing was sent, so no data...
    assert!(data.is_none());

    // ... but if you do send something...
    sender.send(b"some data").unwrap();
 
    // ... now you receive something:
    let data = receiver.recv_timeout(Delay::new(Duration::from_secs(1))).await.unwrap();

    assert_eq!(&*data.unwrap(), b"some data");  
});
```

## `Ctrl+C` and other unexpected events

During some anomalous behavior, the queue might enter an inconsistent state.
This inconsistency is mainly related to the position of the sender and of
the receiver in the queue. Writing to the queue is an atomic operation.
Therefore, unless there is something really wrong with your OS, you should be
fine.

The queue is (almost) guaranteed to save all the most up-to-date metadata
for both receiving and sending parts during a panic. The only exception is
if the saving operation fails. However, this is not the case if the process
receives a signal from the OS. Signals from the OS are not handled
automatically by this library. It is understood that the application
programmer knows best how to handle them. If you chose to close queue on
`Ctrl+C` or other signals, you are in luckSaving both sides of the queue
is [async-signal-safe](https://man7.org/linux/man-pages/man7/signal-safety.7.html)
so you may set up a bare signal hook directly using, for example,
[`signal_hook`](https://docs.rs/signal-hook/), if you are the sort of person
that enjoys `unsafe` code. If not, there are a ton of completely safe
alternatives out there. Choose the one that suits you the best.

Unfortunately, there are times when you get `Aborted` or `Killed`. When this
happens, maybe not everything is lost yet. First of all, you will end up
with a queue that is locked by no process. If you know that the process
owning the locks has indeed past away, you may safely delete the lock files
identified by the `.lock` extension. You will also end up with queue
metadata pointing to an earlier state in time. Is is easy to guess what the
sending metadata should be. Is is the top of the last segment file. However,
things get trickier in the receiver side. You know that it is the greatest
of two positions:

1. the bottom of the smallest segment still present in the directory.

2. the position indicated in the metadata file.

Depending on your use case, this might be information enough so that not all
hope is lost. However, this is all you will get.

If you really want to err on the safer side, you may use `Sender::save`
and `Receiver::save` to periodically back the queue state up. Just choose
you favorite timer implementation and set a simple periodical task up every
hundreds of milliseconds. However, be warned that this is only a _mitigation_
of consistency problems, not a solution.

## Known issues and next steps

* This is a brand new project. Although I have tested it and it will
certainly not implode your computer, don't trust your life on it yet.
* Wastes too much kernel time when the queue is small enough and the sender
sends many frequent small messages non-atomically. You can mitigate that by
writing in batches to the queue.
* There are probably unknown bugs hidden in some corner case. If you find
one, please fill an issue in GitHub. Pull requests and contributions are
also greatly appreciated.
