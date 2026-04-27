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

from pathlib import Path

from lsst.daf.butler import Butler, CollectionInfo

from ..parquet.collections import read_collections_from_parquet
from ..utils.butler_pool import ButlerPool


async def import_collections(butler_pool: ButlerPool, input_file: Path) -> None:
    """Import collections from a parquet file.

    Notes
    -----
    All collection types are registered, but the children of chained
    collections and dataset associations of tagged/calibration collections are
    not set up.
    """
    async for batch in read_collections_from_parquet(input_file):
        await butler_pool.run_with_butler(_import_collections, batch)


def _import_collections(butler: Butler, collections: list[CollectionInfo]) -> None:
    with butler.transaction():
        for c in collections:
            butler.collections.register(c.name, c.type, c.doc)
