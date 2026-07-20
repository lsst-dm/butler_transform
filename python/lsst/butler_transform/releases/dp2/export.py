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
import os
import tempfile
from collections.abc import Callable
from pathlib import Path

import click
import rich.console
import rich.table
from duckdb import ColumnExpression, ConstantExpression, DuckDBPyConnection, DuckDBPyRelation
from pyarrow.parquet import read_metadata, read_schema

from ...export.export_data_release import export_data_release
from ...importer.import_data_release import DataReleaseImportInfo
from ...utils.butler_process_pool import ButlerProcessPool
from ...utils.duckdb import initialize_duckdb_connection, write_duckdb_results_to_parquet

# From
# https://rubinobs.atlassian.net/wiki/spaces/DM/pages/1210908682/All+DP2+data+products
DATASET_TYPES = (
    # 1.3 Non-image, non-qserv table Butler datasets
    "object_scarlet_models",
    "visit_summary",
    "deep_coadd_input_summary",
    "compare_warp_artifact_mask",
    # Property maps
    "deepCoadd_*_consolidated_map_*",
    # 1.4 Image datasets
    "raw",
    "visit_image",
    "deep_coadd",
    "template_coadd",
    "difference_image",
    # 1.5 QSERV Table Products
    "object",
    "isolated_star_stellar_motions",
    "object_shear_all",
    "source",
    "object_forced_source",
    "dia_object_forced_source",
    "dia_object",
    "dia_source",
    "ss_object",
    "ss_source",
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
    "ptc",
    "the_monster_20250219",
    "fgcmLookUpTable",
    "skyMap",
    "standard_passband",
    "pretrainedModelPackage",
)

PROVENANCE_DATASET_TYPES = ("run_provenance",)

TOP_LEVEL_COLLECTION = "LSSTCam/runs/DRP/DP2"


_MAX_BUTLER_CONNECTIONS = 32


@click.command
@click.option(
    "--repo",
    # dp2_prep_future and dp2_prep repositories at USDF are the same underlying
    # repository.  The difference between the two is that dp2_prep_future is
    # configured to use the new lsst.images file format for deep_coadd images,
    # while dp2_prep still uses the legacy format.  See DM-55060 for more
    # information.
    #
    # The official DP2 release uses the new file format, so we point at
    # dp2_prep_future.
    default="dp2_prep_future",
)
def export_dp2(repo: str) -> None:
    output_path = Path.cwd().joinpath("dp2-export")
    asyncio.run(_export_dp2_async(repo, output_path))
    _modify_exported_files(output_path)
    _print_summary(output_path)


async def _export_dp2_async(repo: str, output_path: Path) -> Path:
    async with ButlerProcessPool.from_config(repo, _MAX_BUTLER_CONNECTIONS) as butler_pool:
        await export_data_release(
            butler_pool,
            output_directory=output_path,
            dataset_types=DATASET_TYPES,
            provenance_dataset_types=PROVENANCE_DATASET_TYPES,
            collections=[TOP_LEVEL_COLLECTION],
        )
        return output_path


def _modify_exported_files(export_directory: Path) -> None:
    import_info = DataReleaseImportInfo(export_directory)
    _fix_catalog_dataset_types(import_info)

    with initialize_duckdb_connection() as db:
        _remove_unwanted_skymap(db, import_info)
        _filter_unused_visits(db, import_info)


def _fix_catalog_dataset_types(import_info: DataReleaseImportInfo) -> None:
    """Update the storage class for ``dia_object`` and ``dia_source`` from
    ``DataFrame`` to ``ArrowAstropy``, to give the desired default loading
    behavior for ``Butler.get()``.
    """
    manifest = import_info.manifest
    for dt in manifest.datasets:
        if dt.dataset_type.name in ("dia_object", "dia_source"):
            dt.dataset_type.storageClass = "ArrowAstropy"

    with open(import_info.manifest_path, "wb") as fh:
        fh.write(manifest.model_dump_json(indent=2).encode("utf-8"))


def _filter_unused_visits(db: DuckDBPyConnection, import_info: DataReleaseImportInfo) -> None:
    """Remove visit and exposure dimension records that are present in the
    ``dp2_prep`` Butler repository, but are not referenced by any datasets
    included in the DP2 release.
    """
    # Find all of the visits and exposures directly referenced in data IDs from
    # datasets.
    all_dataset_files = [str(dt.dataset_export_file) for dt in import_info.get_dataset_inputs()]
    all_dataset_tables = db.from_parquet(all_dataset_files, union_by_name=True)
    dataset_visits = all_dataset_tables.select("instrument", "visit").distinct()
    dataset_exposures = all_dataset_tables.select("instrument", "exposure").distinct()

    # Find visits corresponding to exposures from datasets, and vice-versa.
    # This is necessary if, for example, we have raws present in the data
    # release that didn't ultimately produce any visit-keyed datasets
    # downstream.
    visit_definition_table = db.from_parquet(str(import_info.get_dimension_record_input("visit_definition")))
    visit_exposures = visit_definition_table.join(dataset_visits, "instrument, visit").select(
        "instrument", "exposure"
    )
    exposure_visits = visit_definition_table.join(dataset_exposures, "instrument, exposure").select(
        "instrument", "visit"
    )

    # Combine records from datasets with records found from the
    # visit_definition table.
    all_visits = dataset_visits.union(exposure_visits).distinct().set_alias("vis")
    all_exposures = dataset_exposures.union(visit_exposures).distinct().set_alias("exp")

    _modify_parquet_file(
        db,
        import_info.get_dimension_record_input("exposure"),
        lambda rel: (
            rel.set_alias("orig")
            .join(all_exposures, "orig.instrument = exp.instrument AND orig.id = exp.exposure", "semi")
            .order("instrument, id")
        ),
    )
    _modify_parquet_file(
        db,
        import_info.get_dimension_record_input("visit"),
        lambda rel: (
            rel.set_alias("orig")
            .join(all_visits, "orig.instrument = vis.instrument AND orig.id = vis.visit", "semi")
            .order("instrument, id")
        ),
    )
    for file in ("visit_detector_region", "visit_system_membership"):
        _modify_parquet_file(
            db,
            import_info.get_dimension_record_input(file),
            lambda rel: rel.join(all_visits, "instrument, visit", "semi").order("instrument, visit"),
        )
    _modify_parquet_file(
        db,
        import_info.get_dimension_record_input("visit_definition"),
        lambda rel: (
            rel.join(all_exposures, "instrument, exposure", "semi")
            .union(rel.join(all_visits, "instrument, visit", "semi"))
            .distinct()
            .order("instrument, visit")
        ),
    )


def _remove_unwanted_skymap(db: DuckDBPyConnection, import_info: DataReleaseImportInfo) -> None:
    """Remove the legacy v1 skymap, which is present in the dimension records
    and collection but not part of the data release.
    """
    unwanted_skymap = ConstantExpression("lsst_cells_v1")
    # Remove the skymap from dimension records
    _modify_parquet_file(
        db,
        import_info.get_dimension_record_input("skymap"),
        lambda rel: rel.filter(ColumnExpression("name") != unwanted_skymap),
    )
    for f in [
        import_info.get_dimension_record_input("tract"),
        import_info.get_dimension_record_input("patch"),
    ]:
        _modify_parquet_file(db, f, lambda rel: rel.filter(ColumnExpression("skymap") != unwanted_skymap))

    # Remove the skymap from datasets.
    files = import_info.get_dataset_input("skyMap")
    wanted_skymap_datasets = (
        db.from_parquet(str(files.dataset_export_file))
        .filter(ColumnExpression("skymap") != unwanted_skymap)
        .select("dataset_id")
    )
    for f in [files.dataset_export_file, files.association_export_file, files.datastore_export_file]:
        _modify_parquet_file(db, f, lambda rel: rel.join(wanted_skymap_datasets, "dataset_id", "semi"))


def _modify_parquet_file(
    db: DuckDBPyConnection, path: Path, function: Callable[[DuckDBPyRelation], DuckDBPyRelation]
) -> None:
    schema = read_schema(path)
    with tempfile.TemporaryDirectory(dir=path.parent) as tempdir:
        output_temp_file = Path(tempdir) / "temp.parquet"
        output_relation = function(db.from_parquet(str(path)))
        write_duckdb_results_to_parquet(output_relation, str(output_temp_file), schema=schema)
        os.replace(output_temp_file, path)


def _print_summary(export_directory: Path) -> None:
    import_info = DataReleaseImportInfo(export_directory)
    console = rich.console.Console()
    console.print(f"Exported data written to {export_directory}")
    table = rich.table.Table("Dimension", "Record Count", title="Dimension Records")
    for dim, file in sorted(import_info.get_dimension_record_inputs().items(), key=lambda x: x[0].name):
        table.add_row(dim.name, _get_row_count(file))
    console.print(table)

    table = rich.table.Table(
        "Dataset Type", "Datasets", "Datastore Entries", "Associations", title="Datasets"
    )
    for dt in sorted(import_info.get_dataset_inputs(), key=lambda dt: dt.dataset_type.name):
        table.add_row(
            dt.dataset_type.name,
            _get_row_count(dt.dataset_export_file),
            _get_row_count(dt.datastore_export_file),
            _get_row_count(dt.association_export_file),
        )
    console.print(table)


def _get_row_count(parquet_path: Path) -> str:
    return str(read_metadata(parquet_path).num_rows)


if __name__ == "__main__":
    export_dp2()
