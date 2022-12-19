# nakamoto-net-mio

This crate is a nakamoto network reactor implemented using the `mio` library.  It is a drop-in replacement for the default [nakamoto-net-poll](https://github.com/cloudhead/nakamoto/tree/master/net/poll) crate normally used with [nakamoto](https://github.com/cloudhead/nakamoto).  The reason to use this over `nakamoto-net-poll` is purely for better cross-platform support.  At the moment `nakamoto-net-poll` is based on [popol](https://github.com/cloudhead/popol) which is a minimal wrapper around `poll()`.  I believe `popol` will only work on unix based systems.

