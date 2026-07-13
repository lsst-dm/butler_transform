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

import tempfile
import unittest
from pathlib import Path

from lsst.butler_transform.test_utils.datastore import make_test_datastore_parquet
from lsst.butler_transform.transform.get_file_list import get_file_list_from_datastore_export


class TestGetFileList(unittest.TestCase):
    def test_get_file_list(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            temppath = Path(tempdir)
            file_1 = temppath / "1.parquet"
            file_2 = temppath / "2.parquet"
            _write_datastore_file(
                file_1,
                [
                    "a.zip",
                    "b.zip#unzip=a.txt",
                    "c.zip",
                    "d.fits",
                    "e.fits",
                    # Duplicate filename
                    "d.fits",
                    # Same filename, different fragment from above
                    "b.zip#unzip=b.txt",
                ],
            )
            _write_datastore_file(
                file_2,
                [
                    # Duplicate filename in different datastore file, with fragment
                    "c.zip#unzip=c.txt",
                    "g.fits",
                ],
            )
            batches = list(get_file_list_from_datastore_export([str(file_1), str(file_2)]))
            self.assertEqual(len(batches), 1)
            self.assertEqual(sorted(batches[0]), ["a.zip", "b.zip", "c.zip", "d.fits", "e.fits", "g.fits"])

            batches = list(get_file_list_from_datastore_export([str(file_1), str(file_2)], batch_size=5))
            self.assertEqual(len(batches), 2)
            self.assertEqual(len(batches[0]), 5)
            self.assertEqual(len(batches[1]), 1)


def _write_datastore_file(path: Path, filenames: list[str]) -> None:
    make_test_datastore_parquet(path, [{"path": p} for p in filenames])
