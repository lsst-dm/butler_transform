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

from collections import defaultdict
from collections.abc import Sized
from dataclasses import dataclass
from functools import partial
from itertools import groupby
from pathlib import Path
from typing import Callable, Literal

from anyio import create_task_group
from anyio.abc import TaskStatus

from lsst.daf.butler import Butler, CollectionType, DatasetAssociation, DatasetRef, DatasetType, Timespan
from lsst.daf.butler._rubin.datastore_records import DatastoreRecordTable, import_datastore_records_table

from ..parquet.async_parquet_reader import TableReaderBase
from ..parquet.datasets import (
    DatasetAssociationParquetReader,
    DatasetAssociationTable,
    DatasetRefTable,
    DatasetsParquetReader,
)
from ..parquet.datastore import DatastoreParquetReader
from ..utils.butler_pool import ButlerPool


@dataclass(frozen=True)
class DatasetImportStartedEvent:
    dataset_type: str
    total_datasets: int
    event: Literal["started"] = "started"


@dataclass(frozen=True)
class DatasetImportProgressEvent:
    dataset_type: str
    datasets_imported: int
    event: Literal["progress"] = "progress"


@dataclass(frozen=True)
class DatasetImportCompletedEvent:
    dataset_type: str
    event: Literal["completed"] = "completed"


type DatasetImportEvent = DatasetImportStartedEvent | DatasetImportProgressEvent | DatasetImportCompletedEvent


type DatasetImportEventCallback = Callable[[DatasetImportEvent], None]

type DatastoreTransformFunction = Callable[[DatastoreRecordTable], DatastoreRecordTable]
"""Function that will be called to modify each batch of datastore records
before importing them to the Butler.
"""


class DatasetImporter:
    """Imports Butler datasets and associated datastore information."""

    def __init__(self, butler_pool: ButlerPool, dataset_type: DatasetType) -> None:
        self._butler_pool = butler_pool
        self._dataset_type = dataset_type

    async def import_datasets(
        self,
        input_file: Path,
        event_callback: DatasetImportEventCallback,
    ) -> None:
        """Import the `lsst.daf.butler.DatasetRef` information from the given
        parquet file.
        """
        async with DatasetsParquetReader(input_file, self._dataset_type) as reader:
            await self._do_import(reader, event_callback, _import_datasets)

    async def import_datastore(
        self,
        input_file: Path,
        event_callback: DatasetImportEventCallback,
        transform_function: DatastoreTransformFunction | None = None,
    ) -> None:
        """Import datastore records from the given parquet file."""
        import_func = partial(_import_datastore, transform_function=transform_function)
        async with DatastoreParquetReader(input_file) as reader:
            await self._do_import(reader, event_callback, import_func)

    async def import_associations(
        self,
        input_file: Path,
        event_callback: DatasetImportEventCallback,
    ) -> None:
        """Populate TAG and CALIBRATION collections from exported association
        data in the given parquet file.

        Notes
        -----
        It is assumed that the collections to be populated have already been
        created.
        """
        async with DatasetAssociationParquetReader(input_file, self._dataset_type) as reader:
            await self._do_import(reader, event_callback, _import_associations)

    async def _do_import[T: Sized](
        self,
        reader: TableReaderBase[T],
        event_callback: DatasetImportEventCallback,
        import_function: Callable[[Butler, T], None],
    ):
        async with create_task_group() as tg:
            total_datasets = await reader.get_row_count()
            event_callback(
                DatasetImportStartedEvent(dataset_type=self._dataset_type.name, total_datasets=total_datasets)
            )
            async for batch in reader.read():
                await tg.start(self._import_batch, batch, event_callback, import_function)

        event_callback(DatasetImportCompletedEvent(dataset_type=self._dataset_type.name))

    async def _import_batch[T: Sized](
        self,
        batch: T,
        event_callback: DatasetImportEventCallback,
        import_function: Callable[[Butler, T], None],
        *,
        task_status: TaskStatus,
    ) -> None:
        await self._butler_pool.run_with_butler(import_function, batch, task_status=task_status)
        event_callback(
            DatasetImportProgressEvent(dataset_type=self._dataset_type.name, datasets_imported=len(batch))
        )


def _import_datasets(butler: Butler, table: DatasetRefTable):
    refs = table.to_refs()
    # _importDatasets can only import refs from one run at a time.
    grouped_refs = defaultdict[str, list[DatasetRef]](list)
    for ref in refs:
        grouped_refs[ref.run].append(ref)
    for group in grouped_refs.values():
        butler.registry._importDatasets(
            group,
            # Setting expand=False will break things if "live ObsCore" is
            # enabled, but expand=True is unacceptably slow because it does a
            # bunch of queries to look up dimension records for the refs we are
            # inserting.
            expand=False,
            # Assume we are inserting into an empty database to activate a fast
            # path with reduced checks for the insert.
            assume_new=True,
        )


def _import_datastore(
    butler: Butler, table: DatastoreRecordTable, transform_function: DatastoreTransformFunction | None
) -> None:
    if transform_function is not None:
        table = transform_function(table)
    import_datastore_records_table(butler, table)


def _import_associations(butler: Butler, table: DatasetAssociationTable) -> None:
    associations = table.sort_by_collection().to_associations()
    for collection, rows in groupby(associations, _get_collection):
        collection_type = butler.collections.get_info(collection).type
        if collection_type == CollectionType.TAGGED:
            butler.registry.associate(collection, [r.ref for r in rows])
        elif collection_type == CollectionType.CALIBRATION:
            for row in rows:
                timespan = row.timespan
                if timespan is None:
                    timespan = Timespan(None, None)
                butler.registry.certify(collection, [row.ref], timespan)
        else:
            raise ValueError(f"Unexpected collection type '{collection_type}' when importing associations")


def _get_collection(association: DatasetAssociation) -> str:
    return association.collection
