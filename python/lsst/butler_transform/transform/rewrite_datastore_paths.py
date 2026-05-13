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

import re
from collections.abc import Sequence
from typing import Callable, TypedDict

import pyarrow as pa

from lsst.daf.butler import Butler
from lsst.daf.butler._rubin.datastore_records import DatastoreRecordTable
from lsst.daf.butler.arrow_utils import ArrowTableUtils


class DatastoreNameAndPath(TypedDict):
    datastore_name: str
    path: str


def rewrite_datastore_and_path(
    table: DatastoreRecordTable, function: Callable[[Sequence[DatastoreNameAndPath]], None]
):
    original_table = table.to_arrow()
    original_columns = original_table.select(["datastore_name", "path"])
    rows = original_columns.to_pylist()
    function(rows)
    replacement_columns = pa.Table.from_pylist(rows, schema=original_columns.schema)
    output_table = ArrowTableUtils.replace_column(
        original_table, "datastore_name", replacement_columns["datastore_name"]
    )
    output_table = ArrowTableUtils.replace_column(output_table, "path", replacement_columns["path"])
    return DatastoreRecordTable.from_arrow(output_table)


def make_uris_absolute(rows: Sequence[DatastoreNameAndPath], datastore_roots: dict[str, str]) -> None:
    is_already_absolute_pattern = re.compile(r"^[^:/]+://")
    for row in rows:
        path = row["path"]
        if not is_already_absolute_pattern.match(path):
            datastore_name = row["datastore_name"]
            root = datastore_roots.get(datastore_name)
            if root is None:
                raise ValueError(
                    f"No root known for datastore '{datastore_name}'.  Known roots: {list(datastore_roots.keys())}"
                )
            row["path"] = f"{root.rstrip('/')}/{path}"


def map_uris_to_datastores(rows: Sequence[DatastoreNameAndPath], datastore_map: dict[str, str]) -> None:
    lookup = tuple(datastore_map.items())
    for row in rows:
        match_found = False
        path = row["path"]
        for prefix, datastore_name in lookup:
            if path.startswith(prefix):
                row["datastore_name"] = datastore_name
                row["path"] = path.removeprefix(prefix).lstrip("/")
                match_found = True
                break
        if not match_found:
            raise ValueError(f"No datastore mapping configured for URI {path}")


def get_datastore_roots_from_butler(butler: Butler) -> dict[str, str]:
    return {k: str(v) for k, v in butler._datastore.roots.items()}
