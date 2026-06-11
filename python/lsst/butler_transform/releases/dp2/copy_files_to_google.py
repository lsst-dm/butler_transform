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

import click
from google.cloud.storage import Blob, Bucket, Client, transfer_manager

from ...importer.import_data_release import DataReleaseImportInfo
from ...transform.get_file_list import get_file_list_from_datastore_export
from ...transform.rewrite_datastore_paths import map_uri_to_datastore
from ._datastore_map import DP2_DATASTORE_MAP

DATASET_TYPES_TO_COPY = ("deep_coadd", "run_provenance")
DP2_BUCKET_NAME = "butler-us-central1-dp2"
DP2_GOOGLE_PROJECT = "data-curation-prod-fbdb"


@click.command
@click.argument("export_directory")
def copy_dp2_files_to_google(export_directory: str) -> None:
    client = Client(project=DP2_GOOGLE_PROJECT)
    bucket = client.bucket(DP2_BUCKET_NAME)

    datastore_export_files = _get_datastore_export_file_paths(export_directory)
    total = 0
    for batch in get_file_list_from_datastore_export(datastore_export_files):
        print(f"Starting transfer of {len(batch)} files")
        transfer_pairs = [_get_transfer_pair(uri, bucket) for uri in batch]
        transfer_manager.upload_many(
            transfer_pairs,
            skip_if_exists=True,
            raise_exception=True,
            # The default of 8 is too low to max out the 64-core, 256GB RAM,
            # 100Gbps data transfer nodes this will run on.
            max_workers=128,
        )
        total += len(batch)
        print(f"Completed transfer of {len(batch)} files ({total} total so far)")

    print("Transfer complete")


def _get_datastore_export_file_paths(export_directory: str) -> list[str]:
    import_info = DataReleaseImportInfo(export_directory)
    datastore_file_map = {
        info.dataset_type.name: str(info.datastore_export_file)
        for info in import_info.get_dataset_inputs()
        if info.dataset_type.name in DATASET_TYPES_TO_COPY
    }
    missing = set(DATASET_TYPES_TO_COPY) - set(datastore_file_map.keys())
    if missing:
        raise ValueError(f"Requested dataset types not in exported data: {missing}")

    return list(datastore_file_map.values())


def _get_transfer_pair(input_uri: str, bucket: Bucket) -> tuple[str, Blob]:
    input_path = input_uri.removeprefix("file://")
    mapped = map_uri_to_datastore(input_uri, DP2_DATASTORE_MAP)
    output_path = f"{mapped['datastore_name']}/{mapped['path']}"
    output_blob = bucket.blob(output_path)
    return (input_path, output_blob)


if __name__ == "__main__":
    copy_dp2_files_to_google()
