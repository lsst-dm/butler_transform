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

import enum
from collections.abc import AsyncIterator, Iterator

from anyio import to_thread


class _Sentinel(enum.Enum):
    FINISHED = enum.auto()


async def convert_sync_iterator_to_async[T](iterator: Iterator[T]) -> AsyncIterator[T]:
    """Convert a sync ``Iterator`` to an ``AsyncIterator` by running the
    iterator in a thread.

    Notes
    -----
    Each iteration may be handed off to a different thread, so this is not safe to use
    for code that relies on thread-local variables.
    """
    while True:
        value = await to_thread.run_sync(next, iterator, _Sentinel.FINISHED)
        if value is _Sentinel.FINISHED:
            return
        yield value
