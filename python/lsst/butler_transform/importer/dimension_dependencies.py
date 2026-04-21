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

from asyncio import Event
from collections.abc import Iterable

from lsst.daf.butler import DimensionElement


class DimensionDependencyTracker:
    """Tracks completion of dimension record imports so that tasks can wait
    until all of their required dimensions have been imported.
    """

    def __init__(self, dimensions: Iterable[DimensionElement]) -> None:
        dimensions = list(dimensions)
        # Tracks whether a given dimension has been imported yet.
        # Each key is a dimension name.
        self._events: dict[str, Event] = {}
        for el in dimensions:
            if el.has_own_table:
                self._events[el.name] = Event()

    async def wait_until_dependencies_complete(self, dimension: DimensionElement) -> None:
        """Wait until all of the "required" and "implied" dependencies of the
        given dimension have been imported.
        """
        # Need to wait for both "required" and "implied" dimensions here --
        # the dimension record tables have foreign keys for both.
        for dep in dimension.dimensions:
            if (view := dep.viewOf) is not None:
                name = view
            else:
                name = dep.name

            if name != dimension.name:
                await self._events[name].wait()

    def mark_complete(self, dimension: DimensionElement) -> None:
        """Notify the tracking system that the records for the given dimension element
        have been imported.
        """
        self._events[dimension.name].set()
