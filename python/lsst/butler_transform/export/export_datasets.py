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

from collections.abc import Callable, Collection, Iterable
from itertools import batched
from pathlib import Path

from anyio import (
    create_memory_object_stream,
    create_task_group,
    to_thread,
)
from anyio.abc import ObjectReceiveStream, ObjectSendStream, TaskStatus

from lsst.daf.butler import Butler, DatasetId, DatasetType
from lsst.daf.butler._rubin.datastore_records import DatastoreRecordTable, export_datastore_records_table

from ..parquet.datasets import DatasetRefTable, DatasetsParquetWriter, read_dataset_ids
from ..parquet.datastore import DatastoreParquetWriter
from ..utils.butler_thread_pool import ButlerThreadPool
from ..utils.sync_send_stream import SyncSendStream


async def export_datasets(
    butler_pool: ButlerThreadPool,
    dataset_type: DatasetType,
    collections: Iterable[str],
    dataset_path: Path,
    datastore_path: Path,
    run_collection_callback: Callable[[Iterable[str]], None],
) -> None:
    """Export datasets of the given type to two parquet files:
    1. A "datasets" file containing the same information as non-expanded
       `lsst.daf.butler.DatasetRef` instances.
    2. A "datastore" file containing the same information as
       `lsst.daf.butler.datastore.stored_file_info.StoredFileInfo`.
    """

    dataset_ref_send, dataset_ref_recv = create_memory_object_stream[DatasetRefTable](2)

    # Export dataset refs to parquet.
    async with create_task_group() as tg:
        tg.start_soon(
            butler_pool.run_with_butler,
            lambda butler: _query_datasets(
                SyncSendStream(dataset_ref_send), butler, dataset_type, collections
            ),
        )
        tg.start_soon(
            _write_datasets_to_parquet, dataset_ref_recv, dataset_type, dataset_path, run_collection_callback
        )

    # Export datastore records to parquet.
    async with create_task_group() as tg:
        # Look up datastore records associated with the datasets.
        datastore_records_send, datastore_records_recv = create_memory_object_stream[DatastoreRecordTable](2)
        tg.start_soon(
            _fetch_datastore_records,
            butler_pool,
            dataset_path,
            datastore_records_send,
        )

        # Write the datastore records to parquet
        tg.start_soon(_write_datastore_records, datastore_path, datastore_records_recv)

    print(f"{dataset_type.name}: complete")


def _query_datasets(
    output: SyncSendStream[DatasetRefTable],
    butler: Butler,
    dataset_type: DatasetType,
    collections: Iterable[str],
) -> None:
    with output, butler.query() as query:
        results = query.datasets(dataset_type, collections, find_first=True)
        for batch in batched(results, 50_000):
            output.send(DatasetRefTable.from_refs(dataset_type, batch))


async def _write_datasets_to_parquet(
    input: ObjectReceiveStream[DatasetRefTable],
    dataset_type: DatasetType,
    output_path: Path,
    run_collection_callback: Callable[[Iterable[str]], None],
) -> None:
    async with input, DatasetsParquetWriter(output_path, dataset_type) as writer:
        async for refs in input:
            print(f"{dataset_type}: {len(refs.table)} datasets")
            run_collection_callback(refs.get_run_collections())
            await writer.add_refs(refs)


async def _fetch_datastore_records(
    butler_pool: ButlerThreadPool,
    dataset_file: Path,
    output: ObjectSendStream[DatastoreRecordTable],
) -> None:
    async with output, create_task_group() as tg:
        async for dataset_ids in read_dataset_ids(dataset_file):
            await tg.start(_fetch_datastore_record_batch, butler_pool, dataset_ids, output)


async def _fetch_datastore_record_batch(
    butler_pool: ButlerThreadPool,
    dataset_ids: Collection[DatasetId],
    output: ObjectSendStream[DatastoreRecordTable],
    task_status: TaskStatus,
) -> None:
    async with butler_pool.get_butler() as butler:
        task_status.started()
        records = await to_thread.run_sync(export_datastore_records_table, butler, dataset_ids)
        await output.send(records)


async def _write_datastore_records(
    output_file: Path, input: ObjectReceiveStream[DatastoreRecordTable]
) -> None:
    async with input, DatastoreParquetWriter(output_file) as writer:
        async for records in input:
            await writer.write_records(records)
