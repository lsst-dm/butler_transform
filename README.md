# butler_transform

Tools for bulk export and modification of [Butler](https://github.com/lsst/daf_butler) databases.

## Why this library is using anyio instead of stdlib asyncio

The vast majority of I/O in this library involves pushing synchronous blocking
library functions onto a thread pool.  The standard library `asyncio.to_thread`
has a major defect: when cancellation occurs, it detaches the running thread
and returns immediately.  This means that resources acquired outside the thread
cannot be safely used inside the thread, because the async code will continue
on and release the resource while the detached thread is still using it.
`anyio`'s `to_thread`, when combined with its implementation of task groups,
does not suffer from this problem: cancellation waits for the thread to
complete before continuing.

I also found myself re-inventing a wrapper around the standard library
`asyncio.Queue` which was a worse version of `anyio`'s
`MemoryObjectStream`s.

`anyio` is a thin wrapper around the standard library, and is used internally
by other libraries used widely in Rubin (notably FastAPI and httpx), so
hopefully it doesn't prove to be an overly burdensome dependency.
