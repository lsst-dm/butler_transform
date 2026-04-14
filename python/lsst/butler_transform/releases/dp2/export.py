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
