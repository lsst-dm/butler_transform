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

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from lsst.daf.butler._rubin.datastore_records import DatastoreRecordTable

from ...importer.import_datasets import DatastoreTransformFunction
from ...transform.rewrite_datastore_paths import (
    AbsolutePathMapper,
    DatastoreNameAndPath,
    map_absolute_uris_to_datastores,
    rewrite_datastore_and_path,
)
from ._mini_subset import is_in_dp2_mini_subset

DP2_DATASTORE_MAP = {
    "file:///sdf/group/rubin/repo/dp2_prep": "dp2",
    # This is a rucio alias for /sdf/data/rubin/repo/main_20210215/LSSTCam/calib
    "file:///sdf/data/rubin/rses/lsst/butlerdisk/rucio/repo/ancillary/LSSTCam/calib": "calib",
    "file:///sdf/data/rubin/shared/refcats": "refcats",
    "file:///sdf/data/rubin/lsstdata/offline/instrument/LSSTCam": "raw",
}
"""
Storage locations used by the `dp2_prep` Butler repository at USDF, as a
mapping from physical paths to virtual "datastore" names corresponding to S3
buckets used to serve them.
"""

RSP_USDF_S3_DATASTORE_PATHS = {
    "dp2": "s3://slac@rubin-dp2/",
    "calib": "s3://slac@rubin-lsstcam-calibs/",
    "refcats": "s3://slac@rubin-refcats/",
    "raw": "s3://slac@rubin-lsstcam-raw/",
}
"""Root URIs of the USDF storage locations for DP2 data.  The full set of DP2
data is available in these directories.
"""

RSP_GCS_DATASTORE_PATHS = {
    "dp2": "s3://butler-us-central1-dp2/dp2/",
    "refcats": "s3://butler-us-central1-dp2/refcats/",
    # There are currently no calibs or raws in the DP2 GCS bucket, but these
    # locations are where the upload script would put them, if someone changes
    # the requirements later.
    "calib": "s3://butler-us-central1-dp2/calib/",
    "raw": "s3://butler-us-central1-dp2/raw/",
}
"""Root URIs of the Google Cloud Storage locations for DP2 data.  Only the
"early"/"mini" subset of DP2 was copied to these locations.
"""


@dataclass(frozen=True)
class DatastoreSetup:
    datastore_config: dict
    datastore_transform_function: DatastoreTransformFunction


def get_datastore_setup(file_map: Literal["rsp", "usdf", "frdf"]) -> DatastoreSetup:
    if file_map == "rsp":
        return DatastoreSetup(
            datastore_config=generate_rsp_datastore_config(),
            datastore_transform_function=map_table_to_rsp_datastores,
        )
    elif file_map == "usdf":
        return DatastoreSetup(
            datastore_config=generate_usdf_datastore_config(), datastore_transform_function=map_table_for_usdf
        )
    elif file_map == "frdf":
        return DatastoreSetup(
            datastore_config=generate_frdf_datastore_config(), datastore_transform_function=map_table_for_frdf
        )

    raise AssertionError(f"Unknown file mapping {file_map}")


def generate_rsp_datastore_config() -> dict:
    """Generate a Butler datastore configuration to use for DP2 on the Google
    RSP.  The generated configuration is a ChainedDatastore with one child
    datastore for each storage root that will be mapped to an S3 bucket.
    """
    datastores = set(DP2_DATASTORE_MAP.values())
    return {
        "cls": "lsst.daf.butler.datastores.chainedDatastore.ChainedDatastore",
        "datastores": [_generate_file_datastore_config(datastore_name) for datastore_name in datastores],
    }


def _generate_file_datastore_config(datastore_name: str) -> dict:
    return {
        "datastore": {
            "cls": "lsst.daf.butler.datastores.fileDatastore.FileDatastore",
            "name": datastore_name,
            "records": {"table": f"{datastore_name}_datastore_records"},
        }
    }


def map_table_to_rsp_datastores(dataset_type: str, table: DatastoreRecordTable) -> DatastoreRecordTable:
    """Convert the absolute USDF paths in the datastore dump to absolute paths
    reachable from the Google RSP.  Depending on dataset type, some files are
    stored in Google Cloud Storage while others are served via Weka S3 from
    USDF.

    This is the configuration used by the Rubin Science Platform for end users
    on Google Cloud.
    """
    if is_in_dp2_mini_subset(dataset_type):
        map = RSP_GCS_DATASTORE_PATHS
    else:
        map = RSP_USDF_S3_DATASTORE_PATHS

    relative_paths = map_absolute_uris_to_datastores(table, DP2_DATASTORE_MAP)
    return AbsolutePathMapper(map, "dp2").make_paths_absolute(relative_paths)


USDF_DATASTORE_NAME = "dp2"


def generate_usdf_datastore_config() -> dict:
    return _generate_file_datastore_config(USDF_DATASTORE_NAME)


def map_table_for_usdf(dataset_type: str, table: DatastoreRecordTable) -> DatastoreRecordTable:
    """
    Modify the datastore dump to assign all files to a single datastore
    matching the default Butler datastore configuration.  Paths are left
    as absolute URIs. This has the same effect as if the datasets had been
    ingested using the Butler "direct" mode, referencing them from their
    current location in the file system.

    This configuration could be used to set up a DP2 Butler at USDF matching
    the one deployed at the Google RSP.
    """
    return rewrite_datastore_and_path(table, _map_files_for_usdf)


def _map_files_for_usdf(rows: Sequence[DatastoreNameAndPath]) -> None:
    for row in rows:
        # Remap all datasets into a single datastore.
        row["datastore_name"] = USDF_DATASTORE_NAME

        # The paths are absolute URIs to the files at USDF.  If you needed to
        # modify these, you could do something like:
        # row["path"] = row["path"].replace("file:///sdf/group/rubin/repo/dp2_prep", "file:///something_else")


FRDF_DATASTORE_NAME = "dp2"


def generate_frdf_datastore_config() -> dict:
    return _generate_file_datastore_config(FRDF_DATASTORE_NAME)


def map_table_for_frdf(dataset_type: str, table: DatastoreRecordTable) -> DatastoreRecordTable:
    """
    Modify the datastore dump to assign all files to a single datastore
    matching the default Butler datastore configuration.  Paths are left
    as absolute URIs. This has the same effect as if the datasets had been
    ingested using the Butler "direct" mode, referencing them from their
    current location in the file system.

    This configuration could be used to set up a DP2 Butler at USDF matching
    the one deployed at the Google RSP.
    """
    return rewrite_datastore_and_path(table, _map_files_for_frdf)


def _map_files_for_frdf(rows: Sequence[DatastoreNameAndPath]) -> None:
    for row in rows:
        # Remap all datasets into a single datastore.
        row["datastore_name"] = FRDF_DATASTORE_NAME

        # We remap all paths to their absolute URI under the
        # `.../releases/dp2` directory at FrDF. This is the equivalent of
        # ingesting all butler datasets in direct mode.
        path = row["path"]
        if path.startswith("file:///sdf/group/rubin/repo/dp2_prep"):
            # `dp2` category
            row["path"] = path.replace(
                "file:///sdf/group/rubin/repo/dp2_prep",
                "davs://ccdavrubinint.in2p3.fr:5443/pnfs/in2p3.fr/lsst/releases/dp2",
                1,
            )
        elif path.startswith(
            "file:///sdf/data/rubin/rses/lsst/butlerdisk/rucio/repo/ancillary/LSSTCam/calib"
        ):
            # `calib` category
            row["path"] = path.replace(
                "file:///sdf/data/rubin/rses/lsst/butlerdisk/rucio/repo/ancillary/LSSTCam/calib",
                "davs://ccdavrubinint.in2p3.fr:5443/pnfs/in2p3.fr/lsst/releases/ancillary/calib",
                1,
            )
        elif path.startswith("file:///sdf/data/rubin/shared/refcats"):
            # `refcats` category
            row["path"] = path.replace(
                "file:///sdf/data/rubin/shared/refcats",
                "davs://ccdavrubinint.in2p3.fr:5443/pnfs/in2p3.fr/lsst/releases/raw/refcats",
                1,
            )
        elif path.startswith("file:///sdf/data/rubin/lsstdata/offline/instrument/LSSTCam"):
            # `raw` category
            row["path"] = path.replace(
                "file:///sdf/data/rubin/lsstdata/offline/instrument/LSSTCam",
                "davs://ccdavrubinint.in2p3.fr:5443/pnfs/in2p3.fr/lsst/instrument/raw/LSSTCam",
                1,
            )
        else:
            raise ValueError(f"path starting with an unexpected prefix {path}")
