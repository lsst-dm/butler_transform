from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pyarrow
from anyio import CancelScope, to_thread
from pyarrow.parquet import ParquetFile

from ..utils.sync_iterators import convert_sync_iterator_to_async


async def read_parquet_async(
    input_file: str | Path, *, batch_size: int = 10000, columns: list[str] | None = None
) -> AsyncIterator[pyarrow.RecordBatch]:
    """Asynchronously iterate over the rows over a parquet file."""
    reader = await to_thread.run_sync(ParquetFile, input_file)
    try:
        iterator = await to_thread.run_sync(
            lambda: reader.iter_batches(batch_size=batch_size, columns=columns)
        )
        async for batch in convert_sync_iterator_to_async(iterator):
            yield batch
    finally:
        with CancelScope(shield=True):
            await to_thread.run_sync(reader.close)
