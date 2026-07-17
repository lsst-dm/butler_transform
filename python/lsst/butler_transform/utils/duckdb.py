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

import tempfile
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Unpack

import pyarrow
from anyio import TASK_STATUS_IGNORED, CapacityLimiter, to_thread
from anyio.abc import TaskStatus
from duckdb import DuckDBPyConnection, DuckDBPyRelation, connect
from pyarrow.parquet import ParquetWriter

from ..parquet.async_parquet_writer import DEFAULT_COMPRESSION


@contextmanager
def initialize_duckdb_connection(memory_limit: str = "4GB") -> Iterator[DuckDBPyConnection]:
    """Set up an in-memory DuckDB database, applying a memory limit and using
    the system tempdir for its working directory.

    Parameters
    ----------
    memory_limit, optional
        Maximum amount of RAM that DuckDB will use when executing queries.

    Yields
    ------
    connection
        DuckDB connection object.
    """
    with (
        tempfile.TemporaryDirectory(suffix=".duckdb") as tmpdir,
        connect(
            config={
                # DuckDB will use up to 80% of system memory if you don't
                # explicitly set it.
                "memory_limit": memory_limit,
                # DuckDB will use the current working directory if you don't
                # set it, which is frequently a small quota-limited volume on
                # USDF.
                "temp_directory": tmpdir,
                # For paranoia -- some of the scratch volumes at USDF are
                # petabyte-sized.
                "max_temp_directory_size": "128GB",
            }
        ) as conn,
    ):
        yield conn


class DuckDbPool:
    """Utility class for using DuckDB from async code and limiting the number
    of concurrent threads using it, to reduce buffer and CPU contention.

    Notes
    -----
    Users should usually call ``DuckDbPool.initialize()`` instead of
    constructing this class directly.
    """

    def __init__(self, connection: DuckDBPyConnection, max_connections) -> None:
        self._connection = connection
        self._capacity_limiter = CapacityLimiter(max_connections)

    @staticmethod
    @asynccontextmanager
    async def initialize(*, memory_limit: str = "4GB", max_connections=4) -> AsyncIterator[DuckDbPool]:
        """Set up an in-memory DuckDB database instance and return a pool
        configured to use it.
        """
        with initialize_duckdb_connection(memory_limit) as conn:
            yield DuckDbPool(conn, max_connections)

    async def run_with_duckdb[*P, T](
        self,
        func: Callable[[DuckDBPyConnection, *P], T],
        *args: Unpack[P],
        task_status: TaskStatus = TASK_STATUS_IGNORED,
    ) -> T:
        """Wait until a DuckDB connection is available, then run the given
        function in a thread with the connection instance as the first
        argument.
        """
        async with self._capacity_limiter:
            task_status.started()
            return await to_thread.run_sync(self._run, func, *args)

    def _run[*P, T](self, func: Callable[[DuckDBPyConnection, *P], T], *args: Unpack[P]):
        with self._connection.cursor() as conn:
            func(conn, *args)


def write_duckdb_results_to_parquet(
    relation: DuckDBPyRelation,
    output_file: str | Path,
    schema: pyarrow.Schema | None = None,
    row_group_size=100_000,
):
    """Write DuckDB results to a Parquet file with the given schema and row
    group size.

    Notes
    -----
    DuckDB has a built in `to_parquet` method, but it uses a schema inferred
    from the internal DuckDB table.  The built-in method doesn't support
    writing the Arrow extension types currently required by some of the columns
    defined in ``daf_butler``, nor will it apply dictionary encodings by
    default.
    """
    reader = relation.to_arrow_reader(row_group_size)
    try:
        with ParquetWriter(output_file, schema, compression=DEFAULT_COMPRESSION) as writer:
            for batch in reader:
                writer.write_batch(batch.cast(schema))
    finally:
        reader.close()
