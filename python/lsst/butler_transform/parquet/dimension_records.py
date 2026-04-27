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
# along with this program.  If not, see <http://www.gnu.org/licenses/>

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pyarrow

from lsst.daf.butler import DimensionElement, DimensionRecordTable

from .async_parquet_reader import AsyncParquetReader
from .async_parquet_writer import AsyncParquetWriter


class DimensionRecordParquetWriter(AsyncParquetWriter):
    """Writes `lsst.daf.butler.DimensionRecord` rows to a parquet file."""

    def __init__(self, output_path: Path | str, dimension: DimensionElement) -> None:
        schema = DimensionRecordTable.make_arrow_schema(dimension)
        super().__init__(output_path, schema, min_rows_per_write=50_000)
        self._dimension = dimension

    async def add_records_from_table(self, table: DimensionRecordTable) -> None:
        assert table.element == self._dimension, (
            "Dimension records to write should be same as dimensions from writer constructor."
        )
        await self.write_table(table.to_arrow())


class DimensionRecordParquetReader:
    """Reads `lsst.daf.butler.DimensionRecord` rows from a parquet file."""

    def __init__(self, reader: AsyncParquetReader, dimension: DimensionElement) -> None:
        self._reader = reader
        self._dimension = dimension

    @asynccontextmanager
    @staticmethod
    async def create(
        input_file: Path | str, dimension: DimensionElement
    ) -> AsyncIterator[DimensionRecordParquetReader]:
        async with AsyncParquetReader.create(input_file) as reader:
            yield DimensionRecordParquetReader(reader, dimension)

    async def read(self, *, batch_size: int = 50_000) -> AsyncIterator[DimensionRecordTable]:
        schema = DimensionRecordTable.make_arrow_schema(self._dimension)
        async for batch in self._reader.iter_batches(batch_size=batch_size):
            table = pyarrow.Table.from_batches([batch], schema=schema)
            yield DimensionRecordTable(self._dimension, table=table)

    async def get_row_count(self) -> int:
        return await self._reader.get_row_count()
