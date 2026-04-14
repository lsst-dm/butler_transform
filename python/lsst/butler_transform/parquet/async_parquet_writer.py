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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.from collections.abc import Iterable

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
