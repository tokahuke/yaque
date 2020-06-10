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

## Version 0.3.2:

* Typo corrections.
* Small improvements to docs.
* Feature `recovery`: utilities for unlocking locks and to guess send metadata.
Guessing recv metadata is trickier and will be "coming soon".
* Performance improvements to the `Receiver`: buffer input + less lock contention.
This is _quite_ significant in terms of throughput. 