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
from functools import partial
from typing import Callable, TypedDict

import pyarrow as pa

from lsst.daf.butler import Butler
from lsst.daf.butler._rubin.datastore_records import DatastoreRecordTable
from lsst.daf.butler.arrow_utils import ArrowTableUtils


class AbsolutePathMapper:
    """
    Utility to convert paths in a Butler datastore dump to absolute paths.

    Parameters
    ----------
    input_datastore_roots
        Mapping from datastore name to root path URI, which will be used
        to convert relative URIs in the datastore records to absolute URIs.
        Provided automatically if you call ``AbsolutePathMapper.from_butler``
        to instantiate this class.

    Notes
    -----
    Paths in the Butler datastore may be stored as either absolute URLs
    ("direct" ingest mode), or as relative paths mapped under a specified
    datastore root ("copy"/"symlink" and other ingest modes).  Most
    Butler databases contain a mix of both, so this class simplifies things
    downstream by making everything absolute.
    """

    def __init__(self, input_datastore_roots: dict[str, str]) -> None:
        self._input_datastore_roots = input_datastore_roots

    @staticmethod
    def from_butler(butler: Butler) -> AbsolutePathMapper:
        """
        Construct a ``DatastorePathMapper`` with input datastore roots
        retrieved from a Butler configuration.

        Parameters
        ----------
        butler
            The datastore configuration will be retrieved from this Butler.
        """
        return AbsolutePathMapper(_get_datastore_roots_from_butler(butler))

    def make_paths_absolute(self, table: DatastoreRecordTable) -> DatastoreRecordTable:
        """Return a copy of the given ``DatastoreRecordTable`` with all
        relative paths converted to absolute paths.
        """
        return rewrite_datastore_and_path(table, self._remap_rows)

    def _remap_rows(self, rows: Sequence[DatastoreNameAndPath]) -> None:
        _make_uris_absolute(rows, self._input_datastore_roots)

        """
        output_prefix_datastore_mapping
            Mapping from absolute URI prefix to datastore name (e.g.
            ``{ "file://some_root/": "datastore_name" }``, used to assign paths
            to datastores and convert them to relative paths.
        """


class DatastoreNameAndPath(TypedDict):
    datastore_name: str
    path: str


def rewrite_datastore_and_path(
    table: DatastoreRecordTable, function: Callable[[Sequence[DatastoreNameAndPath]], None]
) -> DatastoreRecordTable:
    """Make a copy of a ``DatastoreRecordTable``, rewriting the
    ``datastore_name`` and ``path`` columns using the given function.

    Parameters
    ----------
    table
        Table to be copied.
    function
        Function that will be called with a list of all datastore name and path
        entries in the table, one dictionary per row.  It should modify the
        dictionaries in-place to make any desired edits.
    """
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


def _make_uris_absolute(rows: Sequence[DatastoreNameAndPath], datastore_roots: dict[str, str]) -> None:
    """Modifies the input in place, converting all relative paths to absolute
    paths using the datastore name to look up the root path.

    Parameters
    ----------
    rows
        List of dictionaries containing ``datastore_name`` and ``path`` keys.
    datastore_roots
        Mapping from datastore name to root path.  Should contain an entry for
        all datastore names in the given rows.
    """
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


def map_absolute_uris_to_datastores(
    table: DatastoreRecordTable, datastore_map: dict[str, str]
) -> DatastoreRecordTable:
    """Make a copy of the given ``DatastoreRecordTable``, converting all paths
    to relative paths and assigning datastore names based on the
    ``datastore_map`` parameter.

    Parameters
    ----------
    table
        Input datastore records.
    datastore_map
        Mapping from absolute URI prefix to datastore name (e.g.
        ``{ "file://some_root/": "datastore_name" }``, used to assign paths
        to datastores and convert them to relative paths.
    """
    return rewrite_datastore_and_path(table, partial(_map_uris_to_datastores, datastore_map=datastore_map))


def map_uri_to_datastore(uri: str, datastore_map: dict[str, str]) -> DatastoreNameAndPath:
    for prefix, datastore_name in datastore_map.items():
        if uri.startswith(prefix):
            return {"datastore_name": datastore_name, "path": uri.removeprefix(prefix).lstrip("/")}

    raise ValueError(f"No datastore mapping configured for URI {uri}")


def _map_uris_to_datastores(rows: Sequence[DatastoreNameAndPath], datastore_map: dict[str, str]) -> None:
    """Modifies the input in place.  Converts all paths to relative paths, and
    assigns datastore names based on the ``datastore_map`` parameter.

    Parameters
    ----------
    rows
        List of dictionaries containing ``datastore_name`` and ``path`` keys.
    datastore_map
        Mapping from absolute URI prefix to datastore name (e.g.
        ``{ "file://some_root/": "datastore_name" }``, used to assign paths
        to datastores and convert them to relative paths.
    """
    for row in rows:
        row.update(map_uri_to_datastore(row["path"], datastore_map))


def _get_datastore_roots_from_butler(butler: Butler) -> dict[str, str]:
    """Returns a mapping from datastore name to root path based on the
    datastore configuration in the given Butler.
    """
    return {k: str(v) for k, v in butler._datastore.roots.items()}
