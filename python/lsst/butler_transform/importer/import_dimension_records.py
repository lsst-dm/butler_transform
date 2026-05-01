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

import dataclasses
from collections import defaultdict
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Literal

from anyio import create_task_group
from anyio.abc import TaskStatus

from lsst.daf.butler import Butler, DimensionElement, DimensionRecordTable

from ..parquet.dimension_records import DimensionRecordParquetReader
from ..utils.butler_pool import ButlerPool
from .dimension_dependencies import DimensionDependencyTracker


@dataclasses.dataclass(frozen=True)
class DimensionRecordImportProgress:
    """Track progress for the full dimension record import process."""

    completed_dimensions: int
    total_dimensions: int
    dimension_progress: dict[str, SingleDimensionImportProgress]


type DimensionImportStatus = Literal["working", "complete"]


@dataclasses.dataclass(frozen=True)
class SingleDimensionImportProgress:
    """Track progress for each individual dimension being imported."""

    status: DimensionImportStatus
    completed_rows: int
    total_rows: int


class DimensionRecordImporter:
    """Importer that loads the given dimension record parquet files to a
    Butler database in parallel.

    Parameters
    ----------
    butler_pool
        Pool of Butler connections to which data will be loaded.
    inputs
        Mapping from dimension element to parquet file path for all of the
        data to be loaded.
    progress_callback
        Function that will be called repeatedly during the import process
        to report on progress.
    """

    def __init__(
        self,
        butler_pool: ButlerPool,
        inputs: Mapping[DimensionElement, Path],
        dimension_tracker: DimensionDependencyTracker,
        progress_callback: Callable[[DimensionRecordImportProgress], None] | None = None,
    ) -> None:
        self._butler_pool = butler_pool
        self._inputs = inputs
        self._dimension_tracker = dimension_tracker
        self._progress_tracker = _ImportProgressTracker(progress_callback, len(inputs))

    async def import_(self) -> None:
        """Execute the import."""
        async with create_task_group() as tg:
            for dimension, input_file in self._inputs.items():
                tg.start_soon(self._import_one_dimension, dimension, input_file)

    async def _import_one_dimension(
        self,
        dimension: DimensionElement,
        input_file: Path,
    ) -> None:
        await self._dimension_tracker.wait_until_dependencies_complete(dimension)

        batch_size = 5000
        if dimension.name == "visit":
            # Visit dimensions insert ~40 skypix overlap rows for every record, so
            # smaller batches reduce the transaction size and release Butler
            # instances to the pool more frequently.
            batch_size = 500

        async with create_task_group() as tg:
            async with DimensionRecordParquetReader(input_file, dimension) as reader:
                total_rows = await reader.get_row_count()
                self._progress_tracker.dimension_started(dimension.name, total_rows)
                async for batch in reader.read(batch_size=batch_size):
                    await tg.start(self._import_batch, batch)

        self._dimension_tracker.mark_complete(dimension)
        self._progress_tracker.dimension_complete(dimension.name)

    async def _import_batch(self, batch: DimensionRecordTable, task_status: TaskStatus) -> None:
        dimension = batch.element.name
        await self._butler_pool.run_with_butler(
            _insert_dimension_records, dimension, batch, task_status=task_status
        )
        self._progress_tracker.dimension_progress(dimension, len(batch))


def _insert_dimension_records(butler: Butler, dimension: str, batch: DimensionRecordTable) -> None:
    butler.registry.insertDimensionData(dimension, *tuple(batch))


class _ImportProgressTracker:
    def __init__(
        self, callback: Callable[[DimensionRecordImportProgress], None] | None, dimension_count: int
    ) -> None:
        self._callback = callback
        self._total_dimensions = dimension_count
        self._dimension_status: dict[str, DimensionImportStatus] = {}
        self._completed_rows: defaultdict[str, int] = defaultdict(lambda: 0)
        self._total_rows: defaultdict[str, int] = defaultdict(lambda: 0)

    def dimension_started(self, dimension: str, total_rows: int) -> None:
        self._dimension_status[dimension] = "working"
        self._total_rows[dimension] = total_rows
        self._send_callback()

    def dimension_complete(self, dimension: str) -> None:
        self._dimension_status[dimension] = "complete"
        self._send_callback()

    def dimension_progress(self, dimension: str, completed_rows: int) -> None:
        self._completed_rows[dimension] += completed_rows
        self._send_callback()

    def _send_callback(self) -> None:
        if self._callback is not None:
            completed_dimensions = len([x for x in self._dimension_status.values() if x == "complete"])
            self._callback(
                DimensionRecordImportProgress(
                    completed_dimensions=completed_dimensions,
                    total_dimensions=self._total_dimensions,
                    dimension_progress={
                        k: SingleDimensionImportProgress(
                            status=self._dimension_status[k],
                            completed_rows=self._completed_rows[k],
                            total_rows=self._total_rows[k],
                        )
                        for k in self._dimension_status.keys()
                    },
                )
            )
