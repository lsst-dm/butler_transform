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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.from collections.abc import Iterable

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from pyarrow.parquet import read_table

from lsst.butler_transform.export.export_data_release import export_data_release
from lsst.daf.butler import Butler, DatasetType
from lsst.resources import ResourcePath


class TestDatasetExport(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = self.enterContext(tempfile.TemporaryDirectory())
        Butler.makeRepo(self.repo)
        with Butler.from_config(self.repo, writeable=True) as butler:
            butler.import_(filename="resource://lsst.daf.butler/tests/registry_data/lsstcam-subset.yaml")

    def test_dataset_release_export(self) -> None:
        butler = self.enterContext(Butler.from_config(self.repo, writeable=True, run="runs/abc"))
        dt1 = DatasetType("dt1", ["instrument", "visit"], "int", universe=butler.dimensions)
        dt2 = DatasetType("dt2", ["instrument", "detector"], "int", universe=butler.dimensions)
        dt3 = DatasetType("dt3", ["instrument", "detector"], "int", universe=butler.dimensions)
        butler.registry.registerDatasetType(dt1)
        butler.registry.registerDatasetType(dt2)
        butler.registry.registerDatasetType(dt3)

        ref1 = butler.put(1, "dt1", {"instrument": "LSSTCam", "visit": 2025120200439})
        ref2 = butler.put(20, "dt1", {"instrument": "LSSTCam", "visit": 2025120200440})
        ref3 = butler.put(3, "dt2", {"instrument": "LSSTCam", "detector": 10})

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            # dt2 is passed as a glob to verify that we expand globs.
            asyncio.run(export_data_release(tmpdir_path, self.repo, ["dt1", "d*2", "dt3"], ["runs/abc"]))

            dt1_datasets = read_table(tmpdir_path.joinpath("dt1.datasets.parquet")).to_pylist()
            dt1_datasets.sort(key=lambda row: row["visit"])
            self.assertEqual(len(dt1_datasets), 2)
            self.assertEqual(dt1_datasets[0]["dataset_id"], ref1.id.bytes)
            self.assertEqual(dt1_datasets[0]["run"], "runs/abc")
            self.assertEqual(dt1_datasets[0]["instrument"], "LSSTCam")
            self.assertEqual(dt1_datasets[0]["visit"], 2025120200439)
            self.assertEqual(dt1_datasets[1]["dataset_id"], ref2.id.bytes)
            self.assertEqual(dt1_datasets[1]["run"], "runs/abc")
            self.assertEqual(dt1_datasets[1]["instrument"], "LSSTCam")
            self.assertEqual(dt1_datasets[1]["visit"], 2025120200440)

            dt2_datasets = read_table(tmpdir_path.joinpath("dt2.datasets.parquet")).to_pylist()
            self.assertEqual(len(dt2_datasets), 1)
            self.assertEqual(dt2_datasets[0]["dataset_id"], ref3.id.bytes)
            self.assertEqual(dt2_datasets[0]["run"], "runs/abc")
            self.assertEqual(dt2_datasets[0]["instrument"], "LSSTCam")
            self.assertEqual(dt2_datasets[0]["detector"], 10)

            dt3_datasets = read_table(tmpdir_path.joinpath("dt3.datasets.parquet")).to_pylist()
            self.assertEqual(len(dt3_datasets), 0)

            dt1_datastore = read_table(tmpdir_path.joinpath("dt1.datastore.parquet")).to_pylist()
            dt1_datastore.sort(key=lambda row: row["path"])
            self.assertEqual(len(dt1_datastore), 2)
            self.assertEqual(dt1_datastore[0]["datastore_name"], "FileDatastore@<butlerRoot>")
            self.assertEqual(dt1_datastore[0]["dataset_id"], ref1.id.bytes)
            self.assertEqual(self._get_absolute_datastore_path(dt1_datastore[0]["path"]), butler.getURI(ref1))
            self.assertEqual(dt1_datastore[0]["formatter"], "lsst.daf.butler.formatters.json.JsonFormatter")
            self.assertEqual(dt1_datastore[0]["storage_class"], "int")
            self.assertIsNone(dt1_datastore[0]["component"])
            self.assertEqual(dt1_datastore[0]["file_size"], 1)
            self.assertIsNone(dt1_datastore[0]["checksum"])
            self.assertEqual(dt1_datastore[1]["datastore_name"], "FileDatastore@<butlerRoot>")
            self.assertEqual(dt1_datastore[1]["dataset_id"], ref2.id.bytes)
            self.assertEqual(self._get_absolute_datastore_path(dt1_datastore[1]["path"]), butler.getURI(ref2))
            self.assertEqual(dt1_datastore[1]["formatter"], "lsst.daf.butler.formatters.json.JsonFormatter")
            self.assertEqual(dt1_datastore[1]["storage_class"], "int")
            self.assertIsNone(dt1_datastore[1]["component"])
            self.assertEqual(dt1_datastore[1]["file_size"], 2)
            self.assertIsNone(dt1_datastore[1]["checksum"])

            dt2_datastore = read_table(tmpdir_path.joinpath("dt2.datastore.parquet")).to_pylist()
            self.assertEqual(len(dt2_datastore), 1)
            self.assertEqual(dt2_datastore[0]["datastore_name"], "FileDatastore@<butlerRoot>")
            self.assertEqual(dt2_datastore[0]["dataset_id"], ref3.id.bytes)
            self.assertEqual(self._get_absolute_datastore_path(dt2_datastore[0]["path"]), butler.getURI(ref3))
            self.assertEqual(dt2_datastore[0]["formatter"], "lsst.daf.butler.formatters.json.JsonFormatter")
            self.assertEqual(dt2_datastore[0]["storage_class"], "int")
            self.assertIsNone(dt2_datastore[0]["component"])
            self.assertEqual(dt2_datastore[0]["file_size"], 1)
            self.assertIsNone(dt2_datastore[0]["checksum"])

            dt3_datastore = read_table(tmpdir_path.joinpath("dt3.datastore.parquet")).to_pylist()
            self.assertEqual(len(dt3_datastore), 0)

    def _get_absolute_datastore_path(self, relative_path: str) -> ResourcePath:
        """Given a relative path, return the absolute path to the file under
        the source Butler's datastore root.
        """
        return ResourcePath(Path(self.repo).joinpath(relative_path).absolute())
