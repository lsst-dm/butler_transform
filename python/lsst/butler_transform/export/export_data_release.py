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

import asyncio
from collections.abc import Iterable
from pathlib import Path

from anyio import open_file
from pydantic import BaseModel

from ..export.export_datasets import DatasetExporter, DatasetExportManifest
from ..export.export_dimension_records import export_dimension_records
from ..parquet.collections import CollectionsParquetWriter
from ..utils.butler_pool import ButlerPool
from ..utils.duckdb import DuckDbPool


async def export_data_release(
    butler_pool: ButlerPool,
    output_directory: Path,
    dataset_types: Iterable[str],
    collections: Iterable[str],
) -> None:
    """Bulk export datasets and associated data in a format that can be used to
    bulk load another Butler database for an end-user-facing data release.

    butler_pool
        Pool of Butler instances from which data will be exported.
    output_directory
        Destination directory for parquet files containing export data.
    dataset_types
        List of dataset type names that will be exported.
    collections
        List of collections that datasets will be exported from.
    """

    Path(output_directory).mkdir(exist_ok=True)

    dimensions = await butler_pool.run_with_butler_in_current_process(lambda butler: butler.dimensions)
    async with DuckDbPool.initialize() as duckdb_pool:
        async with asyncio.TaskGroup() as tg:
            dimension_record_export_files: dict[str, str] = {}
            for el in dimensions.elements:
                if el.has_own_table:
                    filename = f"{el.name}.dimension.parquet"
                    dimension_record_export_files[el.name] = filename
                    tg.create_task(
                        export_dimension_records(butler_pool, el, output_directory.joinpath(filename)),
                    )

            dataset_exporter = DatasetExporter(butler_pool, duckdb_pool, output_directory)
            dataset_export_task = tg.create_task(dataset_exporter.export(dataset_types, collections))

    dataset_export_result = dataset_export_task.result()

    collection_filename = "collections.parquet"
    async with CollectionsParquetWriter(output_directory.joinpath(collection_filename)) as collection_writer:
        await collection_writer.write(dataset_export_result.collections_referenced)

    manifest = DataReleaseExportManifest(
        collection_export_file=collection_filename,
        dimension_config=dimensions.dimensionConfig.dump(),
        dimension_record_files=dimension_record_export_files,
        datasets=dataset_export_result.manifests,
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
    datasets: list[DatasetExportManifest]
    """Information about exported datasets."""
