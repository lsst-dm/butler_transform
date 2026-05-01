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
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal

from anyio import create_task_group
from anyio.abc import TaskStatus

from lsst.daf.butler import Butler, DatasetRef, DatasetType

from ..parquet.datasets import DatasetRefTable, DatasetsParquetReader
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


async def import_datasets(
    butler_pool: ButlerPool,
    dataset_type: DatasetType,
    input_file: Path,
    callback: DatasetImportEventCallback,
) -> None:
    """Import the `lsst.daf.butler.DatasetRef` information from the given
    parquet file.
    """
    async with create_task_group() as tg:
        async with DatasetsParquetReader(input_file, dataset_type) as reader:
            total_datasets = await reader.get_row_count()
            callback(DatasetImportStartedEvent(dataset_type=dataset_type.name, total_datasets=total_datasets))
            async for batch in reader.read():
                await tg.start(_import_batch, butler_pool, batch, callback)

    callback(DatasetImportCompletedEvent(dataset_type=dataset_type.name))


async def _import_batch(
    butler_pool: ButlerPool,
    batch: DatasetRefTable,
    callback: DatasetImportEventCallback,
    *,
    task_status: TaskStatus,
) -> None:
    await butler_pool.run_with_butler(_import_datasets, batch, task_status=task_status)
    callback(
        DatasetImportProgressEvent(dataset_type=batch.dataset_type.name, datasets_imported=len(batch.table))
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
