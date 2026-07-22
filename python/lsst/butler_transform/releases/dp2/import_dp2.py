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

from ...importer.import_data_release import DataReleaseImportInfo, import_data_release
from ._datastore_map import generate_dp2_datastore_config, map_files_to_dp2_datastores
from ._mini_subset import DP2_MINI_SUBSET


@click.command
@click.argument("export_directory")
@click.argument("schema")
@click.argument("database_uri")
@click.option("--mini", is_flag=True)
def import_dp2(export_directory: str, schema: str, database_uri: str, mini: bool) -> None:
    """Set up the Butler database for DP2, starting from an empty database.

    Parameters
    ----------
    export_directory
        Path to a directory containing an exported parquet copy of the DP2
        Butler database.
    schema
        Database schema name that the imported database will be written into.
    database_uri
        Postgres URI for the database server that will be written into.
    """
    import_info = DataReleaseImportInfo(export_directory)
    with TemporaryDirectory() as butler_repo:
        # Construct a Butler configuration that has a chained datastore with a
        # root for each DP2 storage location, and the database configuration
        # given on the command line.
        config = Config()
        config["registry", "db"] = database_uri
        config["registry", "namespace"] = schema
        config["datastore"] = generate_dp2_datastore_config()

        # Create a temporary Butler repo to connect us to the database during
        # the import process.
        repo_config = Butler.makeRepo(butler_repo, config, dimensionConfig=import_info.get_dimension_config())
        repo_config.dumpToUri("dp2-butler-config.yaml")

        dataset_types = DP2_MINI_SUBSET if mini else None
        asyncio.run(
            import_data_release(
                butler_repo,
                import_info,
                dataset_types=dataset_types,
                datastore_transform_function=map_files_to_dp2_datastores,
            )
        )


if __name__ == "__main__":
    import_dp2()
