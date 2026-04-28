# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pyarrow
from anyio import CancelScope, to_thread
from pyarrow.parquet import ParquetFile

from ..utils.sync_iterators import convert_sync_iterator_to_async


class AsyncParquetReader:
    """Wraps `pyarrow.ParquetFile` for use with asyncio.."""

    def __init__(self, reader: ParquetFile) -> None:
        self._reader = reader

    @asynccontextmanager
    @staticmethod
    async def create(input_file: str | Path) -> AsyncIterator[AsyncParquetReader]:
        reader = await to_thread.run_sync(ParquetFile, input_file)
        try:
            yield AsyncParquetReader(reader)
        finally:
            with CancelScope(shield=True):
                await to_thread.run_sync(reader.close)

    async def iter_batches(
        self, *, batch_size: int = 10000, columns: list[str] | None = None
    ) -> AsyncIterator[pyarrow.RecordBatch]:
        """Iterate over the parquet file in batches of rows."""
        iterator = await to_thread.run_sync(
            lambda: self._reader.iter_batches(batch_size=batch_size, columns=columns)
        )
        async for batch in convert_sync_iterator_to_async(iterator):
            yield batch

    async def get_row_count(self) -> int:
        """Return the total num of rows contained in the parquet file."""
        metadata = await to_thread.run_sync(lambda: self._reader.metadata)
        return metadata.num_rows
