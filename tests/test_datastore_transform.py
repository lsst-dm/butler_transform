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

import unittest

from lsst.butler_transform.transform.rewrite_datastore_paths import (
    DatastoreNameAndPath,
    _make_uris_absolute,
    _map_uris_to_datastores,
)


class TestDatastorePathMapper(unittest.TestCase):
    def test_make_uris_absolute(self) -> None:
        rows: list[DatastoreNameAndPath] = [
            {"datastore_name": "ds1", "path": "relative/path.txt"},
            {"datastore_name": "ds2", "path": "relative/path2.txt"},
            {"datastore_name": "ds2", "path": "s3://absolute/path.txt"},
        ]
        datastores = {"ds1": "file:///some_root", "ds2": "file:///other_root/"}
        _make_uris_absolute(rows, datastores)
        self.assertEqual(
            rows,
            [
                {"datastore_name": "ds1", "path": "file:///some_root/relative/path.txt"},
                {"datastore_name": "ds2", "path": "file:///other_root/relative/path2.txt"},
                {"datastore_name": "ds2", "path": "s3://absolute/path.txt"},
            ],
        )

    def test_make_uris_absolute_unknown_root(self) -> None:
        rows: list[DatastoreNameAndPath] = [
            {"datastore_name": "unknown_datastore", "path": "relative/path.txt"}
        ]
        datastores = {"datastore1": "file:///some-root"}
        with self.assertRaisesRegex(ValueError, "No root known for datastore"):
            _make_uris_absolute(rows, datastores)

    def test_map_uris_to_datastores(self) -> None:
        rows: list[DatastoreNameAndPath] = [
            {"datastore_name": "FileDatastore", "path": "s3://folder1/path.txt"},
            {"datastore_name": "FileDatastore", "path": "file:///folder2/subdir/path2.txt"},
            {"datastore_name": "FileDatastore", "path": "file:///folder3//sub/path3.txt"},
            {"datastore_name": "FileDatastore", "path": "file:///folder4/path.txt"},
        ]
        mapping = {
            "file:///folder2/subdir": "file_datastore_1",
            "file:///folder4": "file_datastore_1",
            "file:///folder3": "file_datastore_2",
            "s3://folder1": "s3_datastore",
        }
        _map_uris_to_datastores(rows, mapping)
        self.assertEqual(
            rows,
            [
                {"datastore_name": "s3_datastore", "path": "path.txt"},
                {"datastore_name": "file_datastore_1", "path": "path2.txt"},
                {"datastore_name": "file_datastore_2", "path": "sub/path3.txt"},
                {"datastore_name": "file_datastore_1", "path": "path.txt"},
            ],
        )

    def test_map_uris_to_datastores_unknown_root(self) -> None:
        rows: list[DatastoreNameAndPath] = [
            {"datastore_name": "FileDatastore", "path": "s3://folder1/path.txt"},
        ]
        mapping = {"file:///root/": "datastore"}
        with self.assertRaisesRegex(ValueError, "No datastore mapping configured"):
            _map_uris_to_datastores(rows, mapping)
