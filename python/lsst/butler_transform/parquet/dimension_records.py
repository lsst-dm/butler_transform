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

from collections.abc import AsyncIterator
from pathlib import Path

import pyarrow

from lsst.daf.butler import DimensionElement, DimensionRecordTable

from .async_parquet_reader import read_parquet_async
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


async def read_dimension_records_from_parquet(
    dimension: DimensionElement, input_file: Path | str, batch_size=50_000
) -> AsyncIterator[DimensionRecordTable]:
    """Load `lsst.daf.butler.DimensionRecord` rows from a parquet file."""

    schema = DimensionRecordTable.make_arrow_schema(dimension)
    async for batch in read_parquet_async(input_file, batch_size=batch_size):
        table = pyarrow.Table.from_batches([batch], schema=schema)
        yield DimensionRecordTable(dimension, table=table)
