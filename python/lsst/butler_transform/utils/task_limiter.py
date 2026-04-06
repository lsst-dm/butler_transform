from collections.abc import Awaitable

from anyio import CapacityLimiter


class TaskLimiter:
    """Wrap coroutines to limit the number of maximum number executing
    simultaneously.
    """

    def __init__(self, max_tasks: int) -> None:
        self._limiter = CapacityLimiter(max_tasks)

    async def limit[T](self, coroutine: Awaitable[T]) -> T:
        """Run a coroutine, limiting the maximum number of concurrent
        coroutines running."""
        async with self._limiter:
            return await coroutine
