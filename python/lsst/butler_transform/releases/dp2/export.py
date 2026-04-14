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
from pathlib import Path

from ...export.export_data_release import export_data_release

# From
# https://rubinobs.atlassian.net/wiki/spaces/DM/pages/1210908682/All+DP2+data+products
DATASET_TYPES = (
    # 1.3 Non-image, non-qserv table Butler datasets
    "object_scarlet_models",
    # 1.4 Image datasets
    "raw",
    "visit_image",
    "deep_coadd",
    "template_coadd",
    "difference_image",
    # "future_visit_image",  # missing, requested for pilot run only
    # 1.5 QSERV Table Products
    "object",
    "object_parent",
    "isolated_star_stellar_motions",
    "object_shear_all",
    "source",
    "object_forced_source",
    "dia_object_forced_source",
    "dia_object",
    "dia_source",
    "ss_object",
    "ss_source",
    # "current_identifications", missing
    # "numbered_identifications", missing
    "visit_table",
    "visit_detector_table",
    # 1.6 Calibration products and ancillary inputs
    "bfk",
    "camera",
    "dark",
    "bias",
    "defects",
    "flat",
    "linearizer",
    "crosstalk",
    "cti",
    # "illuminationCorrection", missing
    "ptc",
    "the_monster_20250219",
    "fgcmLookUpTable",
    "skyMap",
    "standard_passband",
    # Provenance
    "*_metadata",
    "run_provenance",
)

COLLECTIONS = (
    "LSSTCam/runs/DRP/DP2-pilot/v30_0_4_rc1/DM-54210/stage4",
    "LSSTCam/runs/DRP/DP2-pilot/v30_0_4_rc1/DM-54210/stage3",
    "LSSTCam/runs/DRP/DP2/v30_0_0/DM-53881/stage2",
)


async def export_dp2() -> None:
    await export_data_release(Path.cwd().joinpath("dp2-export"), "dp2_prep", DATASET_TYPES, COLLECTIONS)


if __name__ == "__main__":
    asyncio.run(export_dp2())
