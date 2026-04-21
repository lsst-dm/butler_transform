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

from collections.abc import Mapping
from pathlib import Path

from anyio import create_task_group

from lsst.daf.butler import DimensionElement

from ..parquet.dimension_records import read_dimension_records_from_parquet
from ..utils.butler_pool import ButlerPool
from .dimension_dependencies import DimensionDependencyTracker


async def import_dimension_records(butler_pool: ButlerPool, inputs: Mapping[DimensionElement, Path]) -> None:
    """Import the given dimension record parquet files to a Butler database in
    parallel.

    Parameters
    ----------
    butler_pool
        Pool of Butler connections to which data will be loaded.
    inputs
        Mapping from dimension element to parquet file path for all of the data
        to be loaded.
    """
    tracker = DimensionDependencyTracker(inputs.keys())
    async with create_task_group() as tg:
        for dimension, input_file in inputs.items():
            tg.start_soon(_import_one_dimension, butler_pool, tracker, dimension, input_file)


async def _import_one_dimension(
    butler_pool: ButlerPool,
    tracker: DimensionDependencyTracker,
    dimension: DimensionElement,
    input_file: Path,
) -> None:
    await tracker.wait_until_dependencies_complete(dimension)
    await import_dimension_records_file(butler_pool, dimension, input_file)
    tracker.mark_complete(dimension)


async def import_dimension_records_file(
    butler_pool: ButlerPool, dimension: DimensionElement, input_file: Path
) -> None:
    """Import a single dimension's records from a parquet file to the Butler
    database.  Database inserts are parallelized.
    """
    batch_size = 5000
    if dimension.name == "visit":
        # Visit dimensions insert ~40 skypix overlap rows for every record, so
        # smaller batches reduce the transaction size and release Butler
        # instances to the pool more frequently.
        batch_size = 500
    async with create_task_group() as tg:
        async for batch in read_dimension_records_from_parquet(
            dimension,
            input_file,
            batch_size=batch_size,
        ):
            await tg.start(
                butler_pool.run_with_butler,
                lambda butler, batch=batch: butler.registry.insertDimensionData(dimension, *tuple(batch)),
            )
