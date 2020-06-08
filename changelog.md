# Changelog for Yaque

## Version 0.2.0:

* Removed `async` from `Receiver::open` and `channel`. Instead of awaiting for
the the segment file to be created, the `Receiver` now atomically creates it if
it doesn't exist.

## Version 0.3.0s:

* `clear` is now `try_clear`. The method `clear` will be async and await the
queue to be accessible.
* `RecvGuard` will now *rollback on drop* (best effort). `RecvGuard::commit`
will have to be always invoked explicitely.
* You now may save the state of the queue explicitely in `Sender` and `Receiver`.
You can use this to backup the queue from time to time, which is useful if you
fear your program may abort.
