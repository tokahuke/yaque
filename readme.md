# Yaque: Yet Another QUEue

<a href="https://docs.rs/yaque"><img src="https://docs.rs/yaque/badge.svg"></a>

Yaque is yet another disk-backed persistent queue for Rust. It implements
an SPSC channel using your OS' filesystem. Its main advantages over a simple
`VecDeque<T>` are that
* You are not constrained by your RAM size, just by your disk size. This
means you can store gigabytes of data without getting OOM killed.
* Your data is safe even if you program panics. All the queue state is
written to the disk when the queue is dropped.
* Your data can *persistence*, that is, can exisit thrhough multiple executions
of your program. Think of it as a very rudimentary kind of database.
* You can pass data between two processes.

Yaque is _assynchronous_ and built directly on top of `mio` and `notify`.
It is therefore completely agnostic to the runtime you are using for you
application. It will work smoothly with `tokio`, with `async-std` or any
other executor of your choice.

## Sample usage

To create a new queue, just use the `channel` function, passing a
directory path on which to mount the queue. If the directiory does not exist
on creation, it (and possibly all its parent directories) will be created.
```rust
use yaque::channel;

let (mut sender, mut receiver) = channel("data/my-queue").await.unwrap();
```
You can also use `Sender::open` and `Receiver::open` to open only one half
of the channel, if you need to.

The usage is similar to the MPSC channel in the standard library, except
that the receiving method, `Receiver::recv` is assynchronous. Writing to
the queue with the sender is basically lock-free and atomic.
```rust
sender.send(b"some data").unwrap();
let data = receiver.recv().await.unwrap();

assert_eq!(&*data, b"some data");
```
The returned value `data` is a kind of guard that implements `Deref` and
`DerefMut` on the undelying type.

## `RecvGuard` and transactional behavior

One important thing to notice is that reads from the queue are
_transactional_. The `Receiver::recv` returns a `RecvGuard` that only
commits the dequeing operation, that is, makes it official in the disk,
when dropped. You can override this behavior using `RecvGuard::rollback`,
although this will inccur in one more filesystem operation. If a thread
panics while holding a `RecvGuard`, instead of commiting the dequeueing
operation, it will _try_ to rollback. If the rollback operation is
unsuccessful, the operation will be commited (possibly with data loss),
since to panic while panicking results in the process being aborted, which
is Really Bad. 

## Batches

You can use the `yaque` queue to send and receive batches of data ,
too. The guarantees are the same as with single reads and writes, except
that you may save on OS overhead when you send items, since only one disk
operation is made. See `Sender::send_batch`, `Receiver::recv_batch` and
`Receiver::recv_while` for more information on receiver batches.

## Known issues and next steps

* This is a brand new project. Although I have tested it and it will
certainly not implode your computer, don't trust your life on it yet.
* Wastes too much kernel time when the queue is small enough and the sender
sends many frequent small messages non-atomically.
* I intend to make this an MPSC queue in the future.
* There are probably unknown bugs hidden in some corner case. If you find
one, please fill an issue in GitHub. Pull requests and contributions are
also greatly appreciated.

## Licensing

This project is open source and licenced under the Apache 2.0 licence.