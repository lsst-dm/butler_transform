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

from collections.abc import Iterable, Sequence

import click

from ...importer.import_data_release import DataReleaseImportInfo
from ...transform.get_file_list import get_file_list_from_datastore_export
from ...transform.rewrite_datastore_paths import map_uri_to_datastore
from ...utils.mp_context import get_clean_mp_context
from ._datastore_map import DP2_DATASTORE_MAP
from ._gcs_copy_worker import GcsCopyWorker

DATASET_TYPES_TO_COPY = ("deep_coadd", "run_provenance")
DP2_BUCKET_NAME = "butler-us-central1-dp2"
DP2_GOOGLE_PROJECT = "data-curation-prod-fbdb"


@click.command
@click.argument("export_directory")
def copy_dp2_files_to_google(export_directory: str) -> None:
    """Copy a subset of DP2 artifacts to Google Cloud Storage.

    Parameters
    ----------
    export_directory
        Directory containing exported DP2 Butler metadata.

    Notes
    -----
    Assumes it is being run at USDF, where the absolute URIs in the exported
    files are valid. [Application Default Credentials](
    https://docs.cloud.google.com/docs/authentication/provide-credentials-adc)
    need to be set up externally before running this script, typically by using
    the Google CLI command ``gcloud auth application-default login``.
    """
    # The Google Cloud Storage transfer manager has a built-in process pool
    # implementation, but it doesn't manage memory well (gigabytes of RAM per
    # process) and we need >100 concurrent uploads to saturate the outbound
    # transfer from USDF data transfer nodes.  So fan out to a smaller number
    # of processes, and have each process do multithreaded transfers.
    context = get_clean_mp_context()
    with context.Pool(
        32, initializer=GcsCopyWorker.initialize, initargs=(DP2_GOOGLE_PROJECT, DP2_BUCKET_NAME)
    ) as pool:
        total = 0
        datastore_export_files = _get_datastore_export_file_paths(export_directory)
        transfer_batch_iterable = _convert_paths_to_transfer_pairs(
            get_file_list_from_datastore_export(datastore_export_files, batch_size=100)
        )
        print("Starting transfer")
        for result in pool.imap_unordered(
            GcsCopyWorker.transfer_to_google_cloud_storage, transfer_batch_iterable
        ):
            total += result.uploaded + result.skipped
            print(f"Transferred {result.uploaded} files, skipped {result.skipped} (total {total})")

    print("Transfer complete")


def _get_datastore_export_file_paths(export_directory: str) -> list[str]:
    import_info = DataReleaseImportInfo(export_directory)
    return [
        str(info.datastore_export_file)
        for info in import_info.get_dataset_inputs(subset=DATASET_TYPES_TO_COPY)
    ]


def _convert_paths_to_transfer_pairs(
    iterable: Iterable[Iterable[str]],
) -> Iterable[Sequence[tuple[str, str]]]:
    for batch in iterable:
        yield [_get_transfer_pair(uri) for uri in batch]


def _get_transfer_pair(input_uri: str) -> tuple[str, str]:
    input_path = input_uri.removeprefix("file://")
    mapped = map_uri_to_datastore(input_uri, DP2_DATASTORE_MAP)
    output_path = f"{mapped['datastore_name']}/{mapped['path']}"
    return (input_path, output_path)


if __name__ == "__main__":
    copy_dp2_files_to_google()
