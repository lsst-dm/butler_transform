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

from collections.abc import Iterator

import click
from duckdb import DuckDBPyRelation
from rich.box import MARKDOWN
from rich.console import Console
from rich.filesize import decimal
from rich.progress import track
from rich.table import Table

from lsst.resources import ResourcePath

from ...importer.import_data_release import DataReleaseImportInfo
from ...transform.get_file_list import get_file_list_relation
from ...utils.duckdb import fetch_batches_of_scalars, fetch_scalar, initialize_duckdb_connection
from ...utils.mp_context import get_clean_mp_context
from ._mini_subset import DP2_MINI_SUBSET


@click.command
@click.argument("export_directory")
@click.option(
    "--mini", is_flag=True, help="Show results for 'early'/'mini' subset instead of the full release"
)
def print_release_summary(export_directory: str, mini: bool) -> None:
    """Print a summary of files associated with each dataset type in the release."""
    import_info = DataReleaseImportInfo(export_directory)
    subset = DP2_MINI_SUBSET if mini else None
    dataset_info = sorted(import_info.get_dataset_inputs(subset=subset), key=lambda dt: dt.dataset_type.name)
    table = Table(
        "Dataset Type",
        "Dataset Count",
        "File Count",
        "File Size",
        "File Size (bytes)",
        title="DP2 Datasets",
        box=MARKDOWN,
    )
    console = Console()
    mp_context = get_clean_mp_context()
    with initialize_duckdb_connection() as db, mp_context.Pool(64) as pool:
        for dt in track(dataset_info, description="Summarizing datasets", console=console):
            dataset_count = fetch_scalar(db.read_parquet(str(dt.dataset_export_file)).count("*"))
            datastore_table = db.read_parquet(str(dt.datastore_export_file))
            file_count = fetch_scalar(get_file_list_relation(datastore_table).count("*"))
            total_file_size = fetch_scalar(datastore_table.filter("file_size IS NOT NULL").sum("file_size"))
            if total_file_size is None:
                total_file_size = 0
            for chunk_size in pool.imap_unordered(
                _get_sum_of_file_sizes, _get_files_with_unknown_sizes(datastore_table)
            ):
                total_file_size += chunk_size
            table.add_row(
                dt.dataset_type.name,
                str(dataset_count),
                str(file_count),
                decimal(total_file_size),
                str(total_file_size),
            )
    console.print(table)
    output_filename = "dp2_summary.txt"
    with open(output_filename, "w") as fh:
        Console(file=fh, width=130).print(table)
    console.print("Output written to ", output_filename)


def _get_files_with_unknown_sizes(datastore_table: DuckDBPyRelation) -> Iterator[list[str]]:
    return fetch_batches_of_scalars(
        get_file_list_relation(datastore_table.filter("file_size IS NULL")), batch_size=100
    )


def _get_sum_of_file_sizes(paths: list[str]) -> int:
    total = 0
    for p in paths:
        total += ResourcePath(p, forceDirectory=False).size()
    return total


if __name__ == "__main__":
    print_release_summary()
