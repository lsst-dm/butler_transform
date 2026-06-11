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
# along with this program.  If not, see <http://www.gnu.org/licenses/>

import asyncio
from tempfile import TemporaryDirectory

import click

from lsst.daf.butler import Butler, Config
from lsst.daf.butler._rubin.datastore_records import DatastoreRecordTable

from ...importer.import_data_release import DataReleaseImportInfo, import_data_release
from ...transform.rewrite_datastore_paths import map_absolute_uris_to_datastores
from ._datastore_map import DP2_DATASTORE_MAP


@click.command
@click.argument("export_directory")
@click.argument("database_uri")
@click.option("--schema", default=None)
def import_dp2(export_directory: str, database_uri: str, schema: str | None) -> None:
    import_info = DataReleaseImportInfo(export_directory)
    with TemporaryDirectory() as butler_repo:
        config = Config()
        config["registry", "db"] = database_uri
        if schema is not None:
            config["registry", "namespace"] = schema

        config["datastore", "cls"] = "lsst.daf.butler.datastores.chainedDatastore.ChainedDatastore"
        datastores = set(DP2_DATASTORE_MAP.values())
        config["datastore", "datastores"] = [
            _generate_datastore_config(datastore_name) for datastore_name in datastores
        ]

        repo_config = Butler.makeRepo(butler_repo, config, dimensionConfig=import_info.get_dimension_config())
        repo_config.dumpToUri("dp2-butler-config.yaml")

        asyncio.run(import_data_release(butler_repo, import_info, _map_files_to_dp2_datastores))


def _generate_datastore_config(datastore_name: str) -> dict:
    return {
        "datastore": {
            "cls": "lsst.daf.butler.datastores.fileDatastore.FileDatastore",
            "name": datastore_name,
            "records": {"table": f"{datastore_name}_datastore_records"},
        }
    }


def _map_files_to_dp2_datastores(table: DatastoreRecordTable) -> DatastoreRecordTable:
    """Convert the absolute paths in the datastore dump to relative paths,
    splitting the datasets up among multiple Butler datastores.  Each of these
    datastores corresponds to an S3 bucket that will serve the files to end
    users.
    """
    return map_absolute_uris_to_datastores(table, DP2_DATASTORE_MAP)


if __name__ == "__main__":
    import_dp2()
