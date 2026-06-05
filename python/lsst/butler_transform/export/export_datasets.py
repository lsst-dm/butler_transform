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

import dataclasses
from collections.abc import Iterable
from itertools import batched
from pathlib import Path

from lsst.daf.butler import Butler, DatasetType

from ..parquet.datasets import DatasetRefTable, DatasetsParquetWriter
from ..utils.butler_pool import ButlerPool


@dataclasses.dataclass
class ExportDatasetsResult:
    run_collections_found: set[str]
    """List of run collection names that were referenced by datasets found
    during the export process.
    """


async def export_datasets(
    butler_pool: ButlerPool,
    dataset_type: DatasetType,
    collections: Iterable[str],
    output_parquet_path: Path,
) -> ExportDatasetsResult:
    """Export `lsst.daf.butler.DatasetRef` information to a parquet file.

    Parameters
    ----------
    butler_pool
        Pool of Butler instances used to fetch data.
    dataset_type
        Type of datasets to export.
    collections
        List of collections containing the datasets to be exported.
    output_parquet_path
        Path where we will write a parquet file containing the datastore
        records.

    Returns
    -------
    result
        Result object containing information about the exported datasets.
    """

    run_collections_found = await butler_pool.run_with_butler(
        _export_datasets_sync,
        dataset_type,
        collections,
        output_parquet_path,
    )

    return ExportDatasetsResult(run_collections_found=run_collections_found)


def _export_datasets_sync(
    butler: Butler,
    dataset_type: DatasetType,
    collections: Iterable[str],
    output_parquet_path: Path,
) -> set[str]:
    run_collections_found: set[str] = set()
    with DatasetsParquetWriter(output_parquet_path, dataset_type) as writer, butler.query() as query:
        results = query.datasets(dataset_type, collections, find_first=True)
        for batch in batched(results, 50_000):
            table = DatasetRefTable.from_refs(dataset_type, batch)
            run_collections_found.update(table.get_run_collections())
            writer.add_refs_sync(table)
            print(f"{dataset_type}: {len(table)} datasets")
    return run_collections_found
