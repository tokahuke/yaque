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

* Solved a bug in `recovery::unlock`: the file was not being parse correctly.
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

## Version 0.5.0-pre1:

* `recv_timeout` and `recv_batch_timeout` to allow receiving with timeout.
* `recv_batch` is "atomic in an assynchronous context".
* Now, unlock works even if the process respawns with the same PID.
