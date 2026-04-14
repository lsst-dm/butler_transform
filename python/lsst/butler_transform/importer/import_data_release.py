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

from lsst.butler_transform.importer.import_dimension_records import import_dimension_records
from lsst.daf.butler import Config, DimensionConfig, DimensionElement, DimensionUniverse

from ..export.export_data_release import DataReleaseExportManifest
from ..utils.butler_pool import ButlerPool

_MAX_BUTLER_CONNECTIONS = 32


class DataReleaseImportInfo:
    """Reads top-level metadata about a data release from an export dump
    directory.
    """

    def __init__(self, export_directory: str | Path) -> None:
        self._directory = Path(export_directory)
        manifest_path = self._directory.joinpath("manifest.json")
        self._manifest = DataReleaseExportManifest.model_validate_json(manifest_path.read_text())
        self._universe = DimensionUniverse(self.get_dimension_config())

    def get_dimension_config(self) -> DimensionConfig:
        """Dimension universe configuration for the exported Butler
        repository.
        """
        return DimensionConfig(Config.fromString(self._manifest.dimension_config))

    def get_dimension_record_inputs(self) -> dict[DimensionElement, Path]:
        """Mapping from dimension to parquet file containing records for that
        dimension.
        """
        return {
            self._universe[k]: self._directory.joinpath(v)
            for k, v in self._manifest.dimension_record_files.items()
        }


async def import_data_release(butler_repo: str, import_info: DataReleaseImportInfo) -> None:
    """Import a set of data release parquet files to a Butler repository, given
    an object containing the top-level metadata from an export dump.
    """
    async with ButlerPool.from_config(butler_repo, _MAX_BUTLER_CONNECTIONS, writeable=True) as butler_pool:
        await import_dimension_records(butler_pool, import_info.get_dimension_record_inputs())
