# Changelog for Yaque

## Version 0.2.0:

* Removed `async` from `Receiver::open` and `channel`. Instead of awaiting for
the the segment file to be created, the `Receiver` now atomically creates it if
it doesn't exist.

## Version 0.3.0 (yanked):

* `clear` is now `try_clear`. The method `clear` will be async and await the
queue to be accessible.
* `RecvGuard` will now *rollback on drop* (best effort). `RecvGuard::commit`
will have to be always invoked explicitly.
* You now may save the state of the queue explicitly in `Sender` and `Receiver`.
You can use this to backup the queue from time to time, which is useful if you
fear your program may abort.

## Version 0.3.1:

* Solved a bug in `RecvGuard::into_inner`: it was rolling back instead of
committing.

## Version 0.3.2 (yanked):

* Typo corrections.
* Small improvements to docs.
* Feature `recovery`: utilities for unlocking locks and to guess send metadata.
Guessing recv metadata is trickier and will be "coming soon".
* Performance improvements to the `Receiver`: buffer input + less lock contention.
This is _quite_ significant in terms of throughput. 

## Version 0.3.3:

* Solved a bug in `recovery::unlock`: the file was not being parsed correctly.
* `recovery::unlock` now ignores missing files, as it should.
* Exposed `FileGuard`.

## Version 0.4.0:

* Renamed `Receiver::take_while` to `Receiver::take_until` because that is what 
it does. Also, now the supplied closure must return a future. This allows for,
e.g., timers!

## Version 0.4.1:

* `Receiver::recv` is now really atomic in an async context. Therefore, if you
do not poll the future to completion, no changes will be made to the receiver
state.

## Version 0.4.3:

* Small improvements to docs.
* Unittests will perform correctly after a previous run was interrupted by 
CTRL+C.
* Created the `recovery::recover` function for a "full course" queue recover in
a single command.

## Version 0.5.0:

* `recv_timeout` and `recv_batch_timeout` to allow receiving with timeout.
* `recv_batch` is "atomic in an asynchronous context".
* Now, unlock works even if the process respawns with the same PID.
* Recovery of queue works with two modes: replay, which is the behavior of 
`recovery::recover`, and "with loss", that discards the bottom segment entirely
(a bit extreme, but we will work on that). Use the
`recovery::recover_with_loss` for this option.
* Docs improvements.
* Refactored crate structure.
* Removed sender-side metadata (the `send_metadata` file). Now, the state of the
sender is always inferred as being the top of the top segment. Period. It
happens that having sender-side metadata was a liability (risk of overwriting data).
* Changed in-disk format! However, if you have no items greater than 67MB, you can 
still benefit from compatibility. From now on, items are limited to ~67MB (a bit
tight, I know), but you get parity checking for an extra layer of safety. Expect
future releases to be completely incompatible with present format. Compatibility
is only supported within the same _minor_ version.

## Version 0.5.1:

* Corrected a bug on the `send_metadata` inferrence thingy. Now, all tests are passing.

## Version 0.6.0:

* Changed in-disk format again. This time, things are woefully incompatible. Don't use
a `<0.5.1` queue with `0.6.0`. You will fail miserably. The difference that breaks
compatibility is one extra parity bit flag in the item header. Before `0.6.0`, it was
used to ensure "legacy mode", where no parity checking took place. Now, it is a parity
bit all on itself. This leads to much more robust error detection (up to 2bits,
guaranteed, but you can get lucky with more!).
* Now you can control the sender more finely with `SenderBuilder`. This includes
chosing a segment size that fits your needs and chosing the "maximum size" for the
queue.
* And yes, now you can control maximum queue size so that `yaque` doesn't blow up your
hard drive. This means that some major API changes took place:
    * `Sender::send` becomes `Sender::try_send` and returns a `TrySendError`. The same
    thing happens to `Sender::send_batch`.
    * A _new_ method called `Sender::send` is created that works like good old
    `Sender::send`, except that it is async and `.await`s for the queue to shrink
    below the maximum size. The same thing happens to `Sender::send_batch`.
* You can also just _try_ to receive items, without the need to `.await` anything. For
each fo the receiving methods `recv`, `recv_batch` and `recv_until` you now have the
try versions: `try_recv`, `try_recv_batch`, `try_recv_until`.
* Solved a bug regarding the rollback of batch transactions when crossing over a segment.
Older versions will do a complete mess out of this. The side effect: `commit` now returns
a `Result`, which has to be treated.


# Version 0.6.1:

* Introduced a new invariant: all items have to be read and used by the end of every
transaction. I could not verify if this invariant always holds. Anyway, there is an
assertion in the code to avoid the worse. If you find such a situation, please fill an
issue.
* Dropping the Receiver forced the `state` to be saved, not the `initial_state` (the
state at the begining of the current transaction). Now, `Drop` calls `Receiver::save`
so that the behavior will be always consistent.
* We have a backup strategy for saving the queue! It invlves no asyc stuff, so it will
only be triggered at the end of a transction. The current criterion is: save at every
250 items read or every 350ms, whichever comes first. This should dimiinish greatly
the necessity for external control of the save mechanism.
* Created a `ReceiverBuilder` to allow people to costumize the way the queue is saved.
This includes altering the above defaults or disabling queue saving altogther.


## Version 0.6.2:

* Created a `QueueIter` which allows to iterate through a queue as a list. The interface
is completely synchronous and has less overhead than just using the `Receiver`, besides
leading to clearer code.
* Documented some undocumented items.
* Upgraded dependencies and reduced the total number of them.


## Version 0.6.3:

* Changes to mirror the pre-releases of `notify`.

### Contributors:

* [@netguy204](https://github.com/netguy204)


## Version 0.6.4:

* Update dependencies and fix vulerabilities (dependabot).

### Contributors:

* [@grant0417](https://github.com/grant0417)
