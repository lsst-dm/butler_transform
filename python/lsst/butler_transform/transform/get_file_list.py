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

from collections.abc import Iterator, Sequence

from ..utils.duckdb import initialize_duckdb_connection


def get_file_list_from_datastore_export(
    datastore_export_files: Sequence[str], batch_size=100_000
) -> Iterator[Sequence[str]]:
    """Retrieve the list of URIs to artifact files represented by a given set
    of datastore records.  The list is de-duplicated and paths have URI
    fragments removed, so the output contains one entry for each physical file
    (rather than one entry for each input datastore record.)

    Parameters
    ----------
    datastore_export_files
        List of paths to exported datastore parquet files that filenames will
        be loaded from.
    batch_size
        Chunk size for the batches of returned filenames.

    Yields
    -------
    batch
        A list of filenames, no larger than the given ``batch_size``.
    """
    with initialize_duckdb_connection() as conn:
        cursor = (
            conn.from_parquet(datastore_export_files).select("split_part(path, '#', 1)").distinct().execute()
        )
        while batch := cursor.fetchmany(batch_size):
            yield [row[0] for row in batch]
