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

from collections.abc import Iterable
from pathlib import Path

from anyio import create_task_group, open_file, to_thread
from pydantic import BaseModel

from lsst.daf.butler import CollectionType

from ..export.export_datasets import export_datasets
from ..export.export_dimension_records import export_dimension_records
from ..parquet.collections import CollectionsParquetWriter
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

        # Snapshot the set of collections that we will be querying against.
        resolved_collections = await butler_pool.run_with_butler(
            lambda butler: butler.collections.query_info(
                collections, include_chains=True, flatten_chains=True, include_doc=True
            )
        )
        # Drop chained collections to get only the concrete collections we will search for.
        search_collections = [c.name for c in resolved_collections if c.type != CollectionType.CHAINED]

        # Each individual export task should saturate at least one Butler
        # connection, so limit the concurrency to prevent other resources (e.g.
        # memory and file handles) from being exhausted.
        limiter = TaskLimiter(_MAX_BUTLER_CONNECTIONS)
        async with create_task_group() as tg:
            dimension_record_export_files: dict[str, str] = {}
            for el in dimensions.elements:
                if el.has_own_table:
                    filename = f"{el.name}.dimension.parquet"
                    dimension_record_export_files[el.name] = filename
                    tg.start_soon(
                        limiter.limit,
                        export_dimension_records(butler_pool, el, output_directory.joinpath(filename)),
                    )

            for dt in resolved_dataset_types:
                tg.start_soon(
                    limiter.limit, export_datasets(butler_pool, dt, search_collections, output_directory)
                )

        collection_filename = "collections.parquet"
        async with CollectionsParquetWriter(
            output_directory.joinpath(collection_filename)
        ) as collection_writer:
            await collection_writer.write(resolved_collections)

        manifest = DataReleaseExportManifest(
            collection_export_file=collection_filename,
            dimension_config=dimensions.dimensionConfig.dump(),
            dimension_record_files=dimension_record_export_files,
        )
        async with await open_file(output_directory.joinpath("manifest.json"), "wb") as fh:
            await fh.write(manifest.model_dump_json(indent=2).encode("utf-8"))


class DataReleaseExportManifest(BaseModel):
    """Top-level manifest for a data release export dump."""

    collection_export_file: str
    """Path to Parquet file containing collection information."""
    dimension_config: str
    """`lsst.daf.butler.DimensionConfig` configuration file."""
    dimension_record_files: dict[str, str]
    """Mapping from dimension name to name of parquet file containing the
    dimension records.
    """
