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

from collections.abc import Callable, Iterable
from itertools import batched
from pathlib import Path

from anyio import (
    create_memory_object_stream,
    create_task_group,
)
from anyio.abc import ObjectReceiveStream

from lsst.daf.butler import Butler, DatasetType

from ..parquet.datasets import DatasetRefTable, DatasetsParquetWriter
from ..utils.butler_thread_pool import ButlerThreadPool
from ..utils.sync_send_stream import SyncSendStream


async def export_datasets(
    butler_pool: ButlerThreadPool,
    dataset_type: DatasetType,
    collections: Iterable[str],
    output_parquet_path: Path,
    run_collection_callback: Callable[[Iterable[str]], None],
) -> None:
    """Export `lsst.daf.butler.DatasetRef` information to a parquet file.

    Parameters
    ----------
    butler_pool
        Pool of Butler instances used to fetch data.
    dataset_type
        Type of datasets to export.
    collections
        List of collections containing the datasets to be exported.
    output_parquet_path
        Path where we will write a parquet file containing the datastore
        records.
    run_collection_callback
        Function that will be called with a list of run collections referenced
        by the datasets found during the export process.  This function will be
        called multiple times, and a given collection may be duplicated between
        multiple calls.
    """

    dataset_ref_send, dataset_ref_recv = create_memory_object_stream[DatasetRefTable](2)

    async with create_task_group() as tg:
        tg.start_soon(
            butler_pool.run_with_butler,
            lambda butler: _query_datasets(
                SyncSendStream(dataset_ref_send), butler, dataset_type, collections
            ),
        )
        tg.start_soon(
            _write_datasets_to_parquet,
            dataset_ref_recv,
            dataset_type,
            output_parquet_path,
            run_collection_callback,
        )


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
