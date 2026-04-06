from collections.abc import Iterable
from pathlib import Path

from anyio import create_task_group, to_thread

from ..export.export_datasets import export_datasets
from ..utils.butler_pool import ButlerPool
from ..utils.task_limiter import TaskLimiter

_MAX_BUTLER_CONNECTIONS = 32


async def export_data_release(
    output_directory: Path, butler_repo: str, dataset_types: Iterable[str], collections: Iterable[str]
) -> None:
    """Bulk export datasets in a format that can be used to bulk load another
    Butler database for an end-user-facing data release.
    """
    # By default, AnyIO only allows 40 concurrent threads total.  Each
    # synchronous Butler query consumes a thread, and then we need more threads
    # for miscellaneous file writing I/O.
    to_thread.current_default_thread_limiter().total_tokens = _MAX_BUTLER_CONNECTIONS * 3

    Path(output_directory).mkdir(exist_ok=True)

    async with (
        ButlerPool.from_config(butler_repo, _MAX_BUTLER_CONNECTIONS) as butler_pool,
    ):
        missing_dataset_types: list[str] = []
        resolved_dataset_types = await butler_pool.run_with_butler(
            lambda butler: butler.registry.queryDatasetTypes(
                dataset_types,
                missing=missing_dataset_types,
            )
        )
        if missing_dataset_types:
            raise RuntimeError(f"Required dataset types not present in repository: {missing_dataset_types}")

        # Each individual export task should saturate at least one Butler
        # connection, so limit the concurrency to prevent other resources (e.g.
        # memory and file handles) from being exhausted.
        limiter = TaskLimiter(_MAX_BUTLER_CONNECTIONS)
        async with create_task_group() as tg:
            for dt in resolved_dataset_types:
                tg.start_soon(limiter.limit, export_datasets(butler_pool, dt, collections, output_directory))
