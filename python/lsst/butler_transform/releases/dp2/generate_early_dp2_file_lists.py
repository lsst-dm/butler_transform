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

from pathlib import Path

import click
from pyarrow.parquet import ParquetFile

from ...importer.import_data_release import DataReleaseImportInfo, DatasetImportInfo
from ...transform.get_file_list import get_file_list_from_datastore_export
from ._mini_subset import DP2_MINI_SUBSET


@click.command
@click.argument("input_directory")
@click.argument("output_directory")
def generate_file_lists(input_directory: str, output_directory: str) -> None:
    """Generate plain text lists of files contained in the early DP2 data
    release.

    Parameters
    ----------
    input_directory
        Directory containing exported data release parquet files for DP2.
    output_directory
        Directory where lists of files will be written.
    """
    import_info = DataReleaseImportInfo(input_directory)
    output_path = Path(output_directory)
    output_path.mkdir(parents=True, exist_ok=True)
    dataset_id_dir = output_path / "dataset_ids"
    dataset_id_dir.mkdir()
    file_list_dir = output_path / "files"
    file_list_dir.mkdir()

    dataset_info = import_info.get_dataset_inputs(subset=DP2_MINI_SUBSET)

    for dt in dataset_info:
        print(f"Exporting {dt.dataset_type.name}")
        filename = f"{dt.dataset_type.name}.txt"
        _write_dataset_ids(dt, dataset_id_dir / filename)
        _write_filenames(dt, file_list_dir / filename)


def _write_dataset_ids(dataset_info: DatasetImportInfo, output_file: Path) -> None:
    with open(output_file, "w") as fh:
        with ParquetFile(dataset_info.dataset_export_file) as reader:
            for batch in reader.iter_batches(columns=["dataset_id"]):
                for id in batch.column("dataset_id").to_pylist():
                    fh.write(f"{id}\n")


def _write_filenames(dataset_info: DatasetImportInfo, output_file: Path) -> None:
    with open(output_file, "w") as fh:
        for batch in get_file_list_from_datastore_export([dataset_info.datastore_export_file]):
            for file in batch:
                fh.write(f"{file}\n")


if __name__ == "__main__":
    generate_file_lists()
