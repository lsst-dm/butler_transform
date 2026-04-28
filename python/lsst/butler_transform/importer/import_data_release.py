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

from pathlib import Path

from anyio import create_task_group

from lsst.butler_transform.importer.import_dimension_records import (
    DimensionRecordImporter,
)
from lsst.daf.butler import Config, DimensionConfig, DimensionElement, DimensionUniverse

from ..export.export_data_release import DataReleaseExportManifest
from ..utils.butler_pool import ButlerPool
from ._progress import DataReleaseImportProgressDisplay
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

    def _get_absolute_path(self, suffix: str) -> Path:
        path = self._directory.joinpath(suffix).resolve()
        if self._directory not in path.parents:
            raise ValueError(f"Path {path} escapes the directory we are importing from")
        return path


async def import_data_release(butler_repo: str, import_info: DataReleaseImportInfo) -> None:
    """Import a set of data release parquet files to a Butler repository, given
    an object containing the top-level metadata from an export dump.
    """
    async with ButlerPool.from_config(butler_repo, _MAX_BUTLER_CONNECTIONS, writeable=True) as butler_pool:
        with DataReleaseImportProgressDisplay().run() as progress:
            async with create_task_group() as tg:
                tg.start_soon(
                    DimensionRecordImporter(
                        butler_pool,
                        import_info.get_dimension_record_inputs(),
                        progress.update_dimension_record_progress,
                    ).import_
                )

                await import_collections(butler_pool, import_info.get_collection_input())
                progress.mark_collection_import_complete()
