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

import gc
from collections.abc import Iterable

from google.cloud.storage import Bucket, Client, transfer_manager


class GcsCopyWorker:
    _client: Client | None = None
    _bucket: Bucket

    @classmethod
    def initialize(cls, project: str, bucket: str) -> None:
        cls._client = Client(project=project)
        cls._bucket = cls._client.bucket(bucket)

    @classmethod
    def transfer_to_google_cloud_storage(cls, path_pairs: Iterable[tuple[str, str]]) -> int:
        if cls._client is None:
            raise AssertionError("Worker was not initialized correctly")
        blob_pairs = [(pair[0], cls._bucket.blob(pair[1])) for pair in path_pairs]
        transfer_manager.upload_many(
            blob_pairs,
            skip_if_exists=True,
            raise_exception=True,
            worker_type=transfer_manager.THREAD,
            max_workers=16,
        )

        # The GCS transfer seems to do a small number of large allocations with
        # cycles, so garbage collection runs infrequently and memory usage can
        # climb if we don't explicitly clean up after each batch.
        gc.collect()

        return len(blob_pairs)
