# Changelog for Yaque

## Version 0.2.0:

* Removed `async` from `Receiver::open` and `channel`. Instead of awaiting for
the the segment file to be created, the `Receiver` now atomically creates it if
it doesn't exist.