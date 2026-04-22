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

from collections import defaultdict
from pathlib import Path

from anyio import create_task_group

from lsst.daf.butler import Butler, DatasetRef, DatasetType

from ..parquet.datasets import DatasetRefTable, read_dataset_refs
from ..utils.butler_pool import ButlerPool


async def import_datasets(butler_pool: ButlerPool, dataset_type: DatasetType, input_file: Path) -> None:
    """Import the `lsst.daf.butler.DatasetRef` information from the given
    parquet file.
    """
    async with create_task_group() as tg:
        async for batch in read_dataset_refs(dataset_type, input_file):
            await tg.start(butler_pool.run_with_butler, _import_datasets, batch)


def _import_datasets(butler: Butler, table: DatasetRefTable):
    refs = table.to_refs()
    # _importDatasets can only import refs from one run at a time.
    grouped_refs = defaultdict[str, list[DatasetRef]](list)
    for ref in refs:
        grouped_refs[ref.run].append(ref)
    for group in grouped_refs.values():
        butler.registry._importDatasets(
            group,
            # Setting expand=False will break things if "live ObsCore" is
            # enabled, but expand=True is unacceptably slow because it does a
            # bunch of queries to look up dimension records for the refs we are
            # inserting.
            expand=False,
            # Assume we are inserting into an empty database to activate a fast
            # path with reduced checks for the insert.
            assume_new=True,
        )
