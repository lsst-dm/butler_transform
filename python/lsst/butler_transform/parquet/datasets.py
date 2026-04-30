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

from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from functools import cache
from pathlib import Path

import pyarrow

from lsst.daf.butler import DatasetId, DatasetRef, DatasetType, DimensionGroup

from .async_parquet_reader import AsyncParquetReader
from .async_parquet_writer import AsyncParquetWriter


class DatasetsParquetWriter(AsyncParquetWriter):
    """Writes Butler `lsst.daf.butler.DatasetRef` instances to a parquet file."""

    def __init__(self, output_file: str | Path, dataset_type: DatasetType) -> None:
        super().__init__(output_file, _create_dataset_arrow_schema(dataset_type.dimensions))

    async def add_refs(self, refs: DatasetRefTable) -> None:
        """Append a batch of datasets to the parquet file."""
        await self.write_table(refs.table)


async def read_dataset_ids(input_file: str | Path) -> AsyncIterator[list[DatasetId]]:
    """Read lists of dataset UUIDs from a dataset parquet file."""
    column_name = "dataset_id"
    async with AsyncParquetReader.create(input_file) as reader:
        async for batch in reader.iter_batches(batch_size=50000, columns=[column_name]):
            yield [_convert_parquet_uuid_to_dataset_id(id.as_py()) for id in batch.column(column_name)]


class DatasetsParquetReader:
    """Reads `lsst.daf.butler.DatasetRef` rows from a parquet file."""

    def __init__(self, reader: AsyncParquetReader, dataset_type: DatasetType) -> None:
        self._reader = reader
        self._dataset_type = dataset_type

    @asynccontextmanager
    @staticmethod
    async def create(
        input_file: Path | str, dataset_type: DatasetType
    ) -> AsyncIterator[DatasetsParquetReader]:
        async with AsyncParquetReader.create(input_file) as reader:
            yield DatasetsParquetReader(reader, dataset_type)

    async def read(self, *, batch_size: int = 50_000) -> AsyncIterator[DatasetRefTable]:
        async for batch in self._reader.iter_batches(batch_size=batch_size):
            yield DatasetRefTable(self._dataset_type, batch)

    async def get_row_count(self) -> int:
        return await self._reader.get_row_count()


class DatasetRefTable:
    def __init__(self, dataset_type: DatasetType, table: pyarrow.Table) -> None:
        self.dataset_type = dataset_type
        self.table = table

    @staticmethod
    def from_refs(dataset_type: DatasetType, refs: Iterable[DatasetRef]) -> DatasetRefTable:
        rows = [{**ref.dataId.required, "dataset_id": ref.id.bytes, "run": ref.run} for ref in refs]
        table = pyarrow.Table.from_pylist(rows, schema=_create_dataset_arrow_schema(dataset_type.dimensions))
        return DatasetRefTable(dataset_type, table)

    def to_refs(self) -> list[DatasetRef]:
        return [
            DatasetRef(
                self.dataset_type,
                row,
                row["run"],
                id=_convert_parquet_uuid_to_dataset_id(row["dataset_id"]),
            )
            for row in self.table.to_pylist()
        ]

    def get_run_collections(self) -> list[str]:
        """Returns the list of run collections referenced by the dataset refs
        in this table.
        """
        return self.table.column("run").unique().to_pylist()


@cache
def _create_dataset_arrow_schema(
    dimensions: DimensionGroup, additional_columns: tuple[pyarrow.Field, ...] = ()
) -> pyarrow.Schema:
    fields = [
        pyarrow.field("dataset_id", pyarrow.binary(16), nullable=False),
        pyarrow.field("run", pyarrow.dictionary(pyarrow.int32(), pyarrow.string()), nullable=False),
        *_get_data_id_column_schemas(dimensions),
        *additional_columns,
    ]
    return pyarrow.schema(fields)


def _get_data_id_column_schemas(dimensions: DimensionGroup) -> list[pyarrow.Field]:
    schema = []
    for dimension_name in dimensions.required:
        dimension = dimensions.universe.dimensions[dimension_name]
        data_type = dimension.primary_key.to_arrow().data_type
        if pyarrow.types.is_string(data_type):
            # Data ID string values always have low cardinality, so dictionary
            # encoding helps a lot.
            data_type = pyarrow.dictionary(pyarrow.int32(), data_type)
        field = pyarrow.field(dimension.name, data_type, nullable=False)
        schema.append(field)

    return schema


def _convert_parquet_uuid_to_dataset_id(dataset_id_binary: object) -> DatasetId:
    assert isinstance(dataset_id_binary, bytes), (
        f"Dataset ID expected to be serialized as binary bytes, got {type(dataset_id_binary)}"
    )
    return DatasetId(bytes=dataset_id_binary)
