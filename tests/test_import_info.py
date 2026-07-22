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

import unittest
from pathlib import Path

from lsst.butler_transform.importer.import_data_release import DataReleaseImportInfo


class TestImportInfo(unittest.TestCase):
    def setUp(self) -> None:
        test_directory = Path(__file__).parent.resolve()
        self.data_directory = test_directory / "test_import_info_data"
        self.import_info = DataReleaseImportInfo(self.data_directory)

    def test_get_dataset_inputs(self) -> None:
        inputs = self.import_info.get_dataset_inputs()
        inputs.sort(key=lambda info: info.dataset_type.name)
        self.assertEqual(len(inputs), 3)
        visit_image = inputs[2]
        self.assertEqual(visit_image.dataset_type.name, "visit_image")
        self.assertEqual(visit_image.dataset_type.storageClass_name, "VisitImage")
        self.assertEqual(
            list(visit_image.dataset_type.dimensions.required), ["instrument", "detector", "visit"]
        )
        self.assertEqual(
            visit_image.dataset_export_file, self.data_directory / "visit_image.datasets.parquet"
        )
        self.assertEqual(
            visit_image.datastore_export_file, self.data_directory / "visit_image.datastore.parquet"
        )

        subset_inputs = self.import_info.get_dataset_inputs(["*map*"])
        subset_names = [input.dataset_type.name for input in subset_inputs]
        subset_names.sort()
        self.assertEqual(
            subset_names,
            ["deepCoadd_epoch_consolidated_map_min", "deepCoadd_exposure_time_consolidated_map_sum"],
        )

        with self.assertRaisesRegex(ValueError, "Requested dataset type pattern.*is not present"):
            self.import_info.get_dataset_inputs(subset=["fish*"])

    def test_dimension_record_inputs(self) -> None:
        inputs = list(self.import_info.get_dimension_record_inputs().items())
        inputs.sort(key=lambda x: x[0].name)
        self.assertEqual(len(inputs), 2)
        self.assertEqual(inputs[0][0].name, "instrument")
        self.assertEqual(inputs[0][1], self.data_directory / "instrument.dimension.parquet")
        self.assertEqual(inputs[1][0].name, "skymap")
        self.assertEqual(inputs[1][1], self.data_directory / "skymap.dimension.parquet")

    def test_dimension_universe(self) -> None:
        self.assertEqual(self.import_info.universe.namespace, "daf_butler")
        self.assertEqual(self.import_info.universe.version, 8)

    def test_get_collection_input(self) -> None:
        self.assertEqual(self.import_info.get_collection_input(), self.data_directory / "collections.parquet")
