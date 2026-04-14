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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.from collections.abc import Iterable

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from anyio import CancelScope, to_thread

from lsst.daf.butler import Butler


class ButlerPool:
    """A fixed-size connection pool for `lsst.daf.Butler` instances, for use from
    async functions.

    Notes
    -----
    Use the ``from_config`` context manager instead of calling ``__init__``
    directly.

    All Butler instance functions block, so returned Butler instances should be
    used in a separate thread, and not in the async event loop.
    """

    def __init__(self, butler: Butler, max_connections: int) -> None:
        self._root_butler = butler
        self._butlers: list[Butler] = []
        self._semaphore = asyncio.BoundedSemaphore(max_connections)
        self.max_connections = max_connections

    def _close(self) -> None:
        for butler in self._butlers:
            butler.close()

    @staticmethod
    @asynccontextmanager
    async def from_config(repo: str, max_connections: int) -> AsyncIterator[ButlerPool]:
        root_butler = await to_thread.run_sync(Butler.from_config, repo)
        try:
            pool = ButlerPool(root_butler, max_connections)
            yield pool
        finally:
            with CancelScope(shield=True):
                await to_thread.run_sync(pool._close)
                await to_thread.run_sync(root_butler.close)

    @asynccontextmanager
    async def get_butler(self) -> AsyncIterator[Butler]:
        async with self._semaphore:
            if self._butlers:
                butler = self._butlers.pop()
            else:
                butler = await to_thread.run_sync(self._root_butler.clone)

            try:
                yield butler
            finally:
                self._butlers.append(butler)

    async def run_with_butler[T](self, func: Callable[[Butler], T]) -> T:
        """Wait until a `lsst.daf.Butler` instance is available, then run the
        given function in a thread.
        """
        async with self.get_butler() as butler:
            return await to_thread.run_sync(func, butler)
