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

import uuid
from collections.abc import Iterator, Mapping
from pathlib import Path

import pyarrow

from lsst.daf.butler.datastore.record_data import (
    DatastoreRecordData,
    StoredDatastoreItemInfo,
)
from lsst.daf.butler.datastores.fileDatastore import StoredFileInfo

from .async_parquet_writer import AsyncParquetWriter

type ButlerDatastoreRecords = Mapping[str, DatastoreRecordData]
"""Mapping from Datastore name to the records for that Datastore (as
returned by `lsst.daf.butler.Datastore.export_records`.)
"""


class DatastoreParquetWriter(AsyncParquetWriter):
    """Writes parquet files equivalent to Butler FileDatastore records."""

    def __init__(self, output_path: str | Path) -> None:
        super().__init__(output_path, _create_schema())

    async def write_records(self, records: ButlerDatastoreRecords) -> None:
        """Append records from Butler Datastore."""
        rows = list(_convert_records_from_rows(records))
        if len(rows) == 0:
            return

        batch = pyarrow.RecordBatch.from_pylist(rows, schema=self._schema)
        await self.write_batch(batch)


def _convert_records_from_rows(records: ButlerDatastoreRecords) -> Iterator[dict[str, object]]:
    # The full structure of the export structure used by
    # Datastore.export_records/Datastore.import_records is:
    # dict:
    #   datastore name -> DatastoreRecordData (class):
    #     .records (dict):
    #       DatasetId -> dict:
    #         table name (str) -> list[StoredDatastoreItemInfo]:
    for datastore in records.keys():
        for dataset_id, record in records[datastore].records.items():
            yield from _convert_record(datastore, dataset_id, record)


def _convert_record(
    datastore_name: str,
    dataset_id: uuid.UUID,
    record: dict[str, list[StoredDatastoreItemInfo]],
) -> Iterator[dict[str, object]]:
    if len(record) > 1:
        # The keys in this dict are a "table" name.  No existing Datastore
        # implementation has more than one key here so it's not clear why
        # this is organized like this.
        raise NotImplementedError("Cannot export datastore records with more than one 'table' entry.")
    item_infos = list(record.values())[0]

    for item in item_infos:
        assert isinstance(item, StoredFileInfo), "Exporting records is only supported for FileDatastore"
        row = {
            "datastore_name": datastore_name,
            "dataset_id": dataset_id.bytes,
            **item.to_simple().model_dump(),
        }
        yield row


def _create_schema() -> pyarrow.Schema:
    string_dict = pyarrow.dictionary(pyarrow.int32(), pyarrow.string())
    return pyarrow.schema(
        [
            pyarrow.field("datastore_name", string_dict, nullable=False),
            pyarrow.field("dataset_id", pyarrow.binary(16), nullable=False),
            # These are the fields from the StoredFileInfo Butler datastore
            # records class.
            pyarrow.field("path", pyarrow.string(), nullable=False),
            pyarrow.field("formatter", string_dict, nullable=False),
            pyarrow.field("storage_class", string_dict, nullable=False),
            pyarrow.field("component", string_dict, nullable=True),
            pyarrow.field("checksum", pyarrow.string(), nullable=True),
            pyarrow.field("file_size", pyarrow.int64(), nullable=False),
        ]
    )
