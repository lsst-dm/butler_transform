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

import atexit
import gc
import logging
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import backoff
from google.api_core.exceptions import RetryError
from google.cloud.storage import Bucket, Client

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CopyResult:
    """Reports the result of a file copy batch."""

    uploaded: int
    """Count of files uploaded successfully"""
    skipped: int
    """Count of files skipped because they already exist."""


class GcsCopyWorker:
    """Worker for copying files to Google Cloud Storage, for use in a child
    process of a `concurrent.futures.ProcessPoolExecutor`."""

    _client: Client | None = None
    _bucket: Bucket
    _pool: ThreadPoolExecutor

    @classmethod
    def initialize(cls, project: str, bucket: str) -> None:
        """Initialization function that must be run once by the caller to
        initialize this class at process startup.

        Parameters
        ----------
        project
            Google Cloud project ID containing the bucket where the files will
            be written.
        bucket
            Google Cloud Storage bucket name where the files will be written.
        """
        logging.basicConfig()
        _LOGGER.setLevel("INFO")
        cls._client = Client(project=project)
        cls._bucket = cls._client.bucket(bucket)
        # The HTTP connection pool internal to the GCS library is limited to
        # 10, so stay under that limit.
        cls._pool = ThreadPoolExecutor(8)
        atexit.register(cls._pool.shutdown)

    @classmethod
    def transfer_to_google_cloud_storage(cls, path_pairs: Iterable[tuple[str, str]]) -> CopyResult:
        """Transfers the given files from local storage to Google Cloud Storage.

        Parameters
        ----------
        path_pairs
            List of files to transfer, as tuples of (absolute local path,
            relative remote path).
        """
        if cls._client is None:
            raise AssertionError("Worker was not initialized correctly")

        upload_count = 0
        skip_count = 0
        for uploaded in cls._pool.map(cls._upload_file, *zip(*path_pairs)):
            if uploaded:
                upload_count += 1
            else:
                skip_count += 1

        # The GCS transfer library seems to do large allocations, but
        # infrequently enough that garbage collection doesn't automatically run
        # often. Memory usage can climb if we don't explicitly clean up after
        # each batch.
        gc.collect()

        return CopyResult(uploaded=upload_count, skipped=skip_count)

    @classmethod
    # The GCS module retries internally, but eventually gives up.  For
    # retryable exceptions, it will raise a RetryError, so we continue
    # retrying ourselves until it succeeds.
    @backoff.on_exception(backoff.expo, RetryError, max_value=120, logger=_LOGGER)
    def _upload_file(cls, source_path: str, destination_path: str) -> bool:
        blob = cls._bucket.blob(destination_path)
        # We frequently need to restart/retry the top level copy process (due
        # to infrastructure failures, or because the list of files changed.) So
        # spend a round-trip to avoid burning bandwidth on files that have
        # already been sent.
        if blob.exists(client=cls._client):
            return False

        blob.upload_from_filename(source_path, client=cls._client)
        return True
