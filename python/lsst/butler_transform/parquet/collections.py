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

from collections.abc import Iterable
from pathlib import Path
from typing import AsyncIterator

import pyarrow

from lsst.daf.butler import CollectionInfo

from .async_parquet_reader import AsyncParquetReader
from .async_parquet_writer import AsyncParquetWriter


class CollectionsParquetWriter(AsyncParquetWriter):
    """Writes Butler `lsst.daf.butler.CollectionInfo` instances to a parquet
    file.
    """

    def __init__(self, output_file: str | Path) -> None:
        schema = pyarrow.schema(
            [
                pyarrow.field("name", pyarrow.string(), nullable=False),
                pyarrow.field("type", pyarrow.int8(), nullable=False),
                pyarrow.field("doc", pyarrow.string()),
                pyarrow.field("children", pyarrow.list_(pyarrow.string())),
            ]
        )
        super().__init__(output_file, schema, min_rows_per_write=10_000)

    async def write(self, collections: Iterable[CollectionInfo]) -> None:
        batch = pyarrow.RecordBatch.from_pylist(
            [c.model_dump(include={"name", "type", "doc", "children"}) for c in collections],
            schema=self._schema,
        )
        await self.write_batch(batch)


async def read_collections_from_parquet(
    input_file: str | Path, batch_size=100
) -> AsyncIterator[list[CollectionInfo]]:
    async with AsyncParquetReader.create(input_file) as reader:
        async for batch in reader.iter_batches(batch_size=batch_size):
            yield [CollectionInfo.model_validate(row) for row in batch.to_pylist()]
