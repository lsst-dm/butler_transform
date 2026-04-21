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

from collections.abc import Iterable
from pathlib import Path

from anyio import create_task_group, to_thread

from ..export.export_datasets import export_datasets
from ..export.export_dimension_records import export_dimension_records
from ..utils.butler_pool import ButlerPool
from ..utils.task_limiter import TaskLimiter

_MAX_BUTLER_CONNECTIONS = 32


async def export_data_release(
    output_directory: Path, butler_repo: str, dataset_types: Iterable[str], collections: Iterable[str]
) -> None:
    """Bulk export datasets in a format that can be used to bulk load another
    Butler database for an end-user-facing data release.
    """
    # By default, AnyIO only allows 40 concurrent threads total.  Each
    # synchronous Butler query consumes a thread, and then we need more threads
    # for miscellaneous file writing I/O.
    to_thread.current_default_thread_limiter().total_tokens = _MAX_BUTLER_CONNECTIONS * 3

    Path(output_directory).mkdir(exist_ok=True)

    async with (
        ButlerPool.from_config(butler_repo, _MAX_BUTLER_CONNECTIONS) as butler_pool,
    ):
        missing_dataset_types: list[str] = []
        resolved_dataset_types = await butler_pool.run_with_butler(
            lambda butler: butler.registry.queryDatasetTypes(
                dataset_types,
                missing=missing_dataset_types,
            )
        )
        if missing_dataset_types:
            raise RuntimeError(f"Required dataset types not present in repository: {missing_dataset_types}")

        dimensions = await butler_pool.run_with_butler(lambda butler: butler.dimensions)

        # Each individual export task should saturate at least one Butler
        # connection, so limit the concurrency to prevent other resources (e.g.
        # memory and file handles) from being exhausted.
        limiter = TaskLimiter(_MAX_BUTLER_CONNECTIONS)
        async with create_task_group() as tg:
            for el in dimensions.elements:
                if el.has_own_table:
                    tg.start_soon(
                        limiter.limit,
                        export_dimension_records(
                            butler_pool, el, output_directory.joinpath(f"{el.name}.dimension.parquet")
                        ),
                    )

            for dt in resolved_dataset_types:
                tg.start_soon(limiter.limit, export_datasets(butler_pool, dt, collections, output_directory))
