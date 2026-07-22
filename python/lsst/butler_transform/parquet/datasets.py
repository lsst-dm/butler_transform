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

from collections.abc import AsyncIterator, Iterable, Sequence
from functools import cache
from pathlib import Path
from typing import override

import pyarrow

from lsst.daf.butler import DatasetAssociation, DatasetId, DatasetRef, DatasetType, DimensionGroup, Timespan
from lsst.daf.butler.arrow_utils import TimespanArrowType

from ..utils.arrow import sort_by_dictionary_column
from .async_parquet_reader import AsyncParquetReader, TableReaderBase
from .async_parquet_writer import AsyncParquetWriter


class DatasetsParquetWriter(AsyncParquetWriter):
    """Writes Butler `lsst.daf.butler.DatasetRef` instances to a parquet file."""

    def __init__(self, output_file: str | Path, dataset_type: DatasetType) -> None:
        super().__init__(output_file, _create_dataset_arrow_schema(dataset_type.dimensions))

    def add_refs_sync(self, refs: DatasetRefTable) -> None:
        """Append a batch of datasets to the parquet file."""
        self.write_table_sync(refs.table)


class DatasetAssociationParquetWriter(AsyncParquetWriter):
    """Writes Butler `lsst.daf.butler.DatasetAssociation` instances to a parquet file."""

    def __init__(self, output_file: str | Path, dataset_type: DatasetType) -> None:
        super().__init__(output_file, _create_association_arrow_schema(dataset_type.dimensions))
        self._dataset_type = dataset_type

    def add_associations_sync(self, associations: Sequence[DatasetAssociation]) -> None:
        table = DatasetAssociationTable.from_associations(self._dataset_type, associations)
        self.write_table_sync(table.table)


async def read_dataset_ids(input_file: str | Path) -> AsyncIterator[list[DatasetId]]:
    """Read lists of dataset UUIDs from a dataset parquet file."""
    column_name = "dataset_id"
    async with AsyncParquetReader.create(input_file) as reader:
        async for batch in reader.iter_batches(batch_size=50000, columns=[column_name]):
            yield [_convert_parquet_uuid_to_dataset_id(id.as_py()) for id in batch.column(column_name)]


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

    def __len__(self) -> int:
        return len(self.table)


class DatasetAssociationTable:
    """`pyarrow.Table` representation of `lsst.daf.butler.DatasetAssociation`
    data.
    """

    COLLECTION_FIELD_TYPE = pyarrow.dictionary(pyarrow.int32(), pyarrow.string())
    TIMESPAN_FIELD_TYPE = TimespanArrowType()
    COLLECTION_FIELD = pyarrow.field(
        "collection",
        COLLECTION_FIELD_TYPE,
        nullable=False,
    )
    TIMESPAN_FIELD = pyarrow.field("timespan", TIMESPAN_FIELD_TYPE, nullable=True)

    def __init__(self, dataset_type: DatasetType, table: pyarrow.Table) -> None:
        self.dataset_type = dataset_type
        self.table = table

    @classmethod
    def from_associations(
        cls, dataset_type: DatasetType, associations: Sequence[DatasetAssociation]
    ) -> DatasetAssociationTable:
        """Convert `lsst.daf.butler.DatasetAssociations` objects to an Arrow
        table.
        """
        ref_table = DatasetRefTable.from_refs(dataset_type, [assoc.ref for assoc in associations])
        collection_column = pyarrow.array(
            [assoc.collection for assoc in associations], type=cls.COLLECTION_FIELD_TYPE
        )
        timespan_column = pyarrow.array(
            [_convert_timespan_to_dict(assoc.timespan) for assoc in associations],
            type=cls.TIMESPAN_FIELD_TYPE,
        )
        table = ref_table.table.append_column(cls.COLLECTION_FIELD, collection_column).append_column(
            cls.TIMESPAN_FIELD, timespan_column
        )

        return DatasetAssociationTable(dataset_type, table)

    def to_associations(self) -> Sequence[DatasetAssociation]:
        """Convert this table to a sequence of
        `lsst.daf.butler.DatasetAssociation` instances.
        """
        ref_table = DatasetRefTable(self.dataset_type, self.table)
        refs = ref_table.to_refs()
        collections = self.table.column("collection").to_pylist()
        timespans = self.table.column("timespan").to_pylist()

        return [
            DatasetAssociation(ref, collection, timespan)
            for ref, collection, timespan in zip(refs, collections, timespans)
        ]

    def sort_by_collection(self) -> DatasetAssociationTable:
        """Return a copy of this `DatasetAssociationTable`, sorted by
        collection name in ascending order.
        """
        return DatasetAssociationTable(self.dataset_type, sort_by_dictionary_column(self.table, "collection"))

    def __len__(self) -> int:
        return len(self.table)


class DatasetsParquetReader(TableReaderBase[DatasetRefTable]):
    """Reads `lsst.daf.butler.DatasetRef` rows from a parquet file."""

    def __init__(self, input_file: Path | str, dataset_type: DatasetType) -> None:
        super().__init__(input_file)
        self._dataset_type = dataset_type

    @override
    def _convert_table(self, table: pyarrow.Table) -> DatasetRefTable:
        return DatasetRefTable(self._dataset_type, table)


class DatasetAssociationParquetReader(TableReaderBase[DatasetAssociationTable]):
    def __init__(self, input_file: Path | str, dataset_type: DatasetType) -> None:
        super().__init__(input_file)
        self._dataset_type = dataset_type

    @override
    def _convert_table(self, table: pyarrow.Table) -> DatasetAssociationTable:
        return DatasetAssociationTable(self._dataset_type, table)


@cache
def _create_dataset_arrow_schema(dimensions: DimensionGroup) -> pyarrow.Schema:
    fields = [
        pyarrow.field("dataset_id", pyarrow.binary(16), nullable=False),
        pyarrow.field("run", pyarrow.dictionary(pyarrow.int32(), pyarrow.string()), nullable=False),
        *_get_data_id_column_schemas(dimensions),
    ]
    return pyarrow.schema(fields)


@cache
def _create_association_arrow_schema(dimensions: DimensionGroup) -> pyarrow.Schema:
    return (
        _create_dataset_arrow_schema(dimensions)
        .append(DatasetAssociationTable.COLLECTION_FIELD)
        .append(DatasetAssociationTable.TIMESPAN_FIELD)
    )


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


def _convert_timespan_to_dict(value: Timespan | None) -> dict[str, int] | None:
    """Convert Timespan to a representation that pyarrow understands."""
    return {"begin_nsec": value.nsec[0], "end_nsec": value.nsec[1]} if value is not None else None
