from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Self

import pyarrow
from anyio import CancelScope, to_thread
from pyarrow.parquet import ParquetWriter


class AsyncParquetWriter(AbstractAsyncContextManager):
    """Async wrapper around `pyarrow.parquet.ParquetWriter`."""

    def __init__(self, output_path: str | Path, schema: object) -> None:
        self._output_path = output_path
        self._schema = schema
        self._writer: ParquetWriter | None = None

    async def __aenter__(self) -> Self:
        self._writer = await to_thread.run_sync(
            lambda: ParquetWriter(self._output_path, self._schema, compression="ZSTD")
        )
        return self

    async def write_batch(self, batch: pyarrow.RecordBatch) -> None:
        """Append a batch of records to the parquet file."""
        if self._writer is None:
            raise AssertionError("AsyncParquetWriter context was not entered.")
        await to_thread.run_sync(self._writer.write_batch, batch)

    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        with CancelScope(shield=True):
            if self._writer is not None:
                await to_thread.run_sync(self._writer.close)
