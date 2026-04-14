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

from contextlib import AbstractContextManager
from typing import Any

from anyio import from_thread
from anyio.streams.memory import MemoryObjectSendStream


class SyncSendStream[T](AbstractContextManager):
    """Wraps `anyio.ObjectSendStream` for use in a sync function running
    outside the event loop in a separate thread.
    """

    def __init__(self, stream: MemoryObjectSendStream[T]) -> None:
        self._stream = stream

    def __enter__(self) -> SyncSendStream:
        return self

    def send(self, item: T) -> None:
        from_thread.run(self._stream.send, item)

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        from_thread.run_sync(self._stream.close)
