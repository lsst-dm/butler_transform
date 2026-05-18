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

from collections.abc import Callable, Collection
from pathlib import Path

from anyio import CapacityLimiter, create_memory_object_stream, create_task_group, to_thread
from anyio.abc import ObjectReceiveStream, ObjectSendStream, TaskStatus

from lsst.daf.butler import DatasetId
from lsst.daf.butler._rubin.datastore_records import DatastoreRecordTable, export_datastore_records_table

from ..parquet.datasets import read_dataset_ids
from ..parquet.datastore import DatastoreParquetWriter
from ..utils.butler_pool import ButlerPool

type DatastoreTransformFunction = Callable[[DatastoreRecordTable], DatastoreRecordTable]


class DatastoreExporter:
    """Exports Butler datastore records to parquet.

    Parameters
    ----------
    butler_pool
        Pool of Butler instances used to fetch data.
    transform_function, optional
        Function that will be called to modify each batch of datastore records
        before writing them to parquet.
    """

    def __init__(
        self, butler_pool: ButlerPool, transform_function: DatastoreTransformFunction | None = None
    ) -> None:
        self._butler_pool = butler_pool
        self._transform_function = transform_function
        self._limiter = CapacityLimiter(self._butler_pool.max_connections)

    async def export_from_dataset_parquet(
        self,
        input_parquet_path: Path,
        output_parquet_path: Path,
    ) -> None:
        """Export Butler datastore records to a parquet file.

        Parameters
        ----------
        input_parquet_path
            Parquet file containing a ``dataset_id`` column with dataset UUIDs for
            which we will export datastore records.
        output_parquet_path
            Path where we will write a parquet file containing the datastore
            records.
        """
        # Export datastore records to parquet.
        async with create_task_group() as tg:
            # Look up datastore records associated with the datasets.
            datastore_records_send, datastore_records_recv = create_memory_object_stream[
                DatastoreRecordTable
            ](2)
            tg.start_soon(
                self._fetch_datastore_records,
                input_parquet_path,
                datastore_records_send,
            )

            # Write the datastore records to parquet
            tg.start_soon(_write_datastore_records, output_parquet_path, datastore_records_recv)

    async def _fetch_datastore_records(
        self,
        dataset_file: Path,
        output: ObjectSendStream[DatastoreRecordTable],
    ) -> None:
        async with output, create_task_group() as tg:
            async for dataset_ids in read_dataset_ids(dataset_file):
                await tg.start(self._fetch_datastore_record_batch, dataset_ids, output)

    async def _fetch_datastore_record_batch(
        self,
        dataset_ids: Collection[DatasetId],
        output: ObjectSendStream[DatastoreRecordTable],
        task_status: TaskStatus,
    ) -> None:
        # Limit total number of jobs waiting to put records into the queue to
        # prevent memory exhaustion.
        async with self._limiter:
            records = await self._butler_pool.run_with_butler(
                export_datastore_records_table, dataset_ids, task_status=task_status
            )
            if self._transform_function is not None:
                records = await to_thread.run_sync(self._transform_function, records)
            await output.send(records)


async def _write_datastore_records(
    output_file: Path, input: ObjectReceiveStream[DatastoreRecordTable]
) -> None:
    async with input, DatastoreParquetWriter(output_file) as writer:
        async for records in input:
            await writer.write_records(records)
