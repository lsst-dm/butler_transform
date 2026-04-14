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
from pathlib import Path

import pyarrow

from lsst.daf.butler import DatasetId, DatasetRef, DatasetType, DimensionGroup

from .async_parquet_reader import read_parquet_async
from .async_parquet_writer import AsyncParquetWriter


class DatasetsParquetWriter(AsyncParquetWriter):
    """Writes Butler `lsst.daf.butler.DatasetRef` instances to a parquet file."""

    def __init__(self, output_file: str | Path, dataset_type: DatasetType) -> None:
        super().__init__(output_file, _create_dataset_arrow_schema(dataset_type, []))

    async def add_refs(self, refs: Iterable[DatasetRef]) -> None:
        """Append a batch of datasets to the parquet file."""
        rows = [_convert_ref_to_row(ref) for ref in refs]
        batch = pyarrow.RecordBatch.from_pylist(rows, schema=self._schema)
        await self.write_batch(batch)


async def read_dataset_ids(input_file: str | Path) -> AsyncIterator[list[DatasetId]]:
    """Read lists of dataset UUIDs from a dataset parquet file."""
    column_name = "dataset_id"
    async for batch in read_parquet_async(input_file, batch_size=50000, columns=[column_name]):
        yield [_convert_parquet_uuid_to_dataset_id(id.as_py()) for id in batch.column(column_name)]


def _convert_ref_to_row(ref: DatasetRef) -> dict[str, object]:
    row: dict[str, object] = dict(ref.dataId.required)
    row["dataset_id"] = ref.id.bytes
    row["run"] = ref.run
    return row


def _create_dataset_arrow_schema(
    dataset_type: DatasetType, additional_columns: list[pyarrow.Field]
) -> pyarrow.Schema:
    fields = [
        pyarrow.field("dataset_id", pyarrow.binary(16), nullable=False),
        pyarrow.field("run", pyarrow.dictionary(pyarrow.int32(), pyarrow.string()), nullable=False),
        *_get_data_id_column_schemas(dataset_type.dimensions),
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
