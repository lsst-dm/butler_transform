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

import asyncio
from collections.abc import AsyncIterator, Callable
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager
from typing import Unpack

from anyio import TASK_STATUS_IGNORED
from anyio.abc import TaskStatus

from lsst.daf.butler import Butler

from .butler_pool import ButlerPool
from .butler_thread_pool import ButlerThreadPool
from .mp_context import get_clean_mp_context


class ButlerProcessPool(ButlerPool):
    """Provides a fixed-size pool of child processes that can be used to
    execute work that needs access to an `lsst.daf.Butler` instance.
    """

    def __init__(
        self, executor: ProcessPoolExecutor, thread_pool: ButlerThreadPool, max_connections: int
    ) -> None:
        self._executor = executor
        self._semaphore = asyncio.BoundedSemaphore(max_connections)
        self.max_connections = max_connections
        self._thread_pool = thread_pool

    @asynccontextmanager
    @staticmethod
    async def from_config(
        repo: str, max_connections: int, writeable: bool = False
    ) -> AsyncIterator[ButlerProcessPool]:
        context = get_clean_mp_context(preload_modules=["lsst.daf.butler"])

        async with ButlerThreadPool.from_config(repo, max_connections, writeable) as thread_pool:
            with ProcessPoolExecutor(
                max_workers=max_connections,
                initializer=_ButlerProcessPoolWorker.initialize,
                initargs=(repo, writeable),
                mp_context=context,
            ) as executor:
                yield ButlerProcessPool(executor, thread_pool, max_connections)

    async def run_with_butler[*P, T](
        self,
        func: Callable[[Butler, *P], T],
        *args: Unpack[P],
        task_status: TaskStatus = TASK_STATUS_IGNORED,
    ) -> T:
        """Run the given function in a child process, with a Butler instance
        provided as its first argument.  The function and all of its arguments
        must be pickleable.
        """
        async with self._semaphore:
            task_status.started()
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, _ButlerProcessPoolWorker.run_with_butler, func, args
            )

    async def run_with_butler_in_current_process[*P, T](
        self,
        func: Callable[[Butler, *P], T],
        *args: Unpack[P],
        task_status: TaskStatus = TASK_STATUS_IGNORED,
    ) -> T:
        async with self._semaphore:
            return await self._thread_pool.run_with_butler(func, *args, task_status=task_status)


class _ButlerProcessPoolWorker:
    """Sets up reusable state for functions run from ``ButlerProcessPool``."""

    _butler: Butler | None = None

    @classmethod
    def initialize(cls, butler_repo: str, writeable: bool) -> None:
        cls._butler = Butler.from_config(butler_repo, writeable=writeable)

    @classmethod
    def run_with_butler[T](cls, function: Callable[..., T], args: tuple) -> T:
        if cls._butler is None:
            raise AssertionError("Process pool was not initialized")
        return function(cls._butler, *args)
