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

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from anyio import CapacityLimiter, create_task_group

from lsst.butler_transform.importer.import_datasets import import_datasets
from lsst.butler_transform.importer.import_dimension_records import (
    DimensionRecordImporter,
)
from lsst.daf.butler import Butler, Config, DatasetType, DimensionConfig, DimensionElement, DimensionUniverse

from ..export.export_data_release import DataReleaseExportManifest
from ..utils.butler_pool import ButlerPool
from ._progress import DataReleaseImportProgressDisplay
from .dimension_dependencies import DimensionDependencyTracker
from .import_collections import import_collections

_MAX_BUTLER_CONNECTIONS = 32


class DataReleaseImportInfo:
    """Reads top-level metadata about a data release from an export dump
    directory.
    """

    def __init__(self, export_directory: str | Path) -> None:
        self._directory = Path(export_directory).resolve()
        manifest_path = self._get_absolute_path("manifest.json")
        self.manifest = DataReleaseExportManifest.model_validate_json(manifest_path.read_text())
        self.universe = DimensionUniverse(self.get_dimension_config())

    def get_dimension_config(self) -> DimensionConfig:
        """Dimension universe configuration for the exported Butler
        repository.
        """
        return DimensionConfig(Config.fromString(self.manifest.dimension_config))

    def get_dimension_record_inputs(self) -> dict[DimensionElement, Path]:
        """Mapping from dimension to parquet file containing records for that
        dimension.
        """
        return {
            self.universe[k]: self._get_absolute_path(v)
            for k, v in self.manifest.dimension_record_files.items()
        }

    def get_collection_input(self) -> Path:
        return self._get_absolute_path(self.manifest.collection_export_file)

    def get_dataset_inputs(self) -> list[DatasetImportInfo]:
        return [
            DatasetImportInfo(
                dataset_type=DatasetType.from_simple(ds.dataset_type, self.universe),
                dataset_export_file=self._get_absolute_path(ds.dataset_export_file),
                datastore_export_file=self._get_absolute_path(ds.datastore_export_file),
            )
            for ds in self.manifest.datasets
        ]

    def _get_absolute_path(self, suffix: str) -> Path:
        path = self._directory.joinpath(suffix).resolve()
        if self._directory not in path.parents:
            raise ValueError(f"Path {path} escapes the directory we are importing from")
        return path


@dataclass(frozen=True)
class DatasetImportInfo:
    dataset_type: DatasetType
    dataset_export_file: Path
    datastore_export_file: Path


async def import_data_release(butler_repo: str, import_info: DataReleaseImportInfo) -> None:
    """Import a set of data release parquet files to a Butler repository, given
    an object containing the top-level metadata from an export dump.
    """
    async with ButlerPool.from_config(butler_repo, _MAX_BUTLER_CONNECTIONS, writeable=True) as butler_pool:
        dataset_inputs = import_info.get_dataset_inputs()
        with DataReleaseImportProgressDisplay(dataset_type_count=len(dataset_inputs)).run() as progress:
            # Registering dataset types creates tables and indexes, so do it first
            # to avoid thinking about what the locking implications might be if we
            # did it in parallel.
            await butler_pool.run_with_butler(
                _register_dataset_types, [input.dataset_type for input in dataset_inputs]
            )
            progress.mark_dataset_type_complete()

            dimension_inputs = import_info.get_dimension_record_inputs()
            dimension_tracker = DimensionDependencyTracker(import_info.universe, dimension_inputs.keys())
            async with create_task_group() as tg:
                # Import dimension records.
                tg.start_soon(
                    DimensionRecordImporter(
                        butler_pool,
                        import_info.get_dimension_record_inputs(),
                        dimension_tracker,
                        progress.update_dimension_record_progress,
                    ).import_
                )

                # Import collections before datasets, because datasets
                # reference run collections.
                await import_collections(butler_pool, import_info.get_collection_input())
                progress.mark_collection_import_complete()

                # Import datasets.
                limiter = CapacityLimiter(_MAX_BUTLER_CONNECTIONS)
                for ds in dataset_inputs:
                    tg.start_soon(
                        _import_datasets_when_ready, butler_pool, ds, limiter, dimension_tracker, progress
                    )


def _register_dataset_types(butler: Butler, dataset_types: Iterable[DatasetType]) -> None:
    for dt in dataset_types:
        butler.registry.registerDatasetType(dt)


async def _import_datasets_when_ready(
    butler_pool: ButlerPool,
    info: DatasetImportInfo,
    limiter: CapacityLimiter,
    dimension_tracker: DimensionDependencyTracker,
    progress_display: DataReleaseImportProgressDisplay,
) -> None:
    await dimension_tracker.wait_until_required_dimensions_complete(info.dataset_type.dimensions)
    async with limiter:
        await import_datasets(
            butler_pool,
            info.dataset_type,
            info.dataset_export_file,
            progress_display.handle_dataset_import_event,
        )
