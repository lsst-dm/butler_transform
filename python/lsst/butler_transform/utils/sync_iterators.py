import enum
from collections.abc import AsyncIterator, Iterator

from anyio import to_thread


class _Sentinel(enum.Enum):
    FINISHED = enum.auto()


async def convert_sync_iterator_to_async[T](iterator: Iterator[T]) -> AsyncIterator[T]:
    """Convert a sync ``Iterator`` to an ``AsyncIterator` by running the
    iterator in a thread.

    Notes
    -----
    Each iteration may be handed off to a different thread, so this is not safe to use
    for code that relies on thread-local variables.
    """
    while True:
        value = await to_thread.run_sync(next, iterator, _Sentinel.FINISHED)
        if value is _Sentinel.FINISHED:
            return
        yield value
