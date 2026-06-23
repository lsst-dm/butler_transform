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

import dataclasses
from collections.abc import Iterable, Sequence
from itertools import batched
from pathlib import Path

from anyio import CapacityLimiter, TemporaryDirectory, create_task_group
from anyio.abc import TaskStatus
from duckdb import DuckDBPyConnection
from pyarrow.parquet import read_schema
from pydantic import BaseModel

from lsst.daf.butler import (
    Butler,
    CollectionInfo,
    CollectionType,
    DatasetAssociation,
    DatasetType,
    SerializedDatasetType,
)

from ..parquet.datasets import DatasetAssociationParquetWriter, DatasetRefTable, DatasetsParquetWriter
from ..utils.butler_pool import ButlerPool
from ..utils.duckdb import DuckDbPool, write_duckdb_results_to_parquet
from .export_datastore import DatastoreExporter


class DatasetExporter:
    def __init__(
        self, butler_pool: ButlerPool, duckdb_pool: DuckDbPool, output_directory: Path | str
    ) -> None:
        self._butler_pool = butler_pool
        self._duckdb_pool = duckdb_pool
        self._output_directory = Path(output_directory)
        # Prevent file handle and memory exhaustion by limiting the total
        # number of exports running simultaneously.
        self._limiter = CapacityLimiter(butler_pool.max_connections)
        self._datastore_exporter = DatastoreExporter(self._butler_pool)

    async def export(
        self,
        dataset_types: Iterable[str],
        provenance_dataset_types: Iterable[str],
        collections: Iterable[str],
    ) -> DatasetExportResult:
        resolved_dataset_types = await self._butler_pool.run_with_butler(
            _resolve_dataset_types, list(dataset_types)
        )

        # Snapshot the set of collections that we will be querying against.
        resolved_collections = await self._butler_pool.run_with_butler(
            _resolve_collections, list(collections)
        )

        context = _DatasetExportContext()
        async with create_task_group() as tg:
            for dt in resolved_dataset_types:
                # For most pipeline outputs, we want to apply find-first so
                # that we take only the "best" version from the front of the
                # collection chain.  For calibration dataset types, there will
                # be multiple versions associated with different certification
                # timespans, and we need to fetch all of them.
                find_first = not dt.isCalibration()
                await tg.start(
                    self._export_single_dataset_type, context, dt, resolved_collections, find_first
                )

        output_collections = list(resolved_collections)
        extra_run_collections = context.run_collections_found - set([c.name for c in resolved_collections])
        if extra_run_collections:
            output_collections.extend(
                await self._butler_pool.run_with_butler(_resolve_collections, extra_run_collections)
            )

        # We need to fetch the provenance datasets from all run collections
        # used by any datasets found during the search.  This requires us to
        # wait until the rest of the dataset searches have finished, and then
        # use the complete list of collections including the "extra" run
        # collections found during the search.
        resolved_provenance_dataset_types = await self._butler_pool.run_with_butler(
            _resolve_dataset_types, list(provenance_dataset_types)
        )
        async with create_task_group() as tg:
            for dt in resolved_provenance_dataset_types:
                await tg.start(
                    self._export_single_dataset_type,
                    context,
                    dt,
                    output_collections,
                    # Don't use find-first -- provenance dataset types generally have empty data IDs,
                    # and we need all of them.
                    False,
                )

        return DatasetExportResult(manifests=context.manifests, collections_referenced=output_collections)

    async def _export_single_dataset_type(
        self,
        context: _DatasetExportContext,
        dataset_type: DatasetType,
        collections: Sequence[CollectionInfo],
        find_first: bool,
        *,
        task_status: TaskStatus,
    ) -> None:
        async with self._limiter:
            task_status.started()

            # Drop chained collections to get only the concrete collections we will search for.
            search_collections = [c.name for c in collections if c.type != CollectionType.CHAINED]
            association_collections = [
                c.name
                for c in collections
                if (c.type == CollectionType.CALIBRATION or c.type == CollectionType.TAGGED)
            ]

            manifest = context.add_manifest(dataset_type)
            dataset_parquet_path = self._output_directory.joinpath(manifest.dataset_export_file)
            run_collections_found = await self._butler_pool.run_with_butler(
                _export_datasets_sync,
                dataset_type,
                find_first,
                search_collections,
                dataset_parquet_path,
            )
            context.add_found_run_collections(run_collections_found)

            async with create_task_group() as tg:
                tg.start_soon(
                    self._datastore_exporter.export_from_dataset_parquet,
                    dataset_parquet_path,
                    self._output_directory.joinpath(manifest.datastore_export_file),
                )
                tg.start_soon(
                    self._export_associations,
                    association_collections,
                    dataset_type,
                    dataset_parquet_path,
                    self._output_directory.joinpath(manifest.association_export_file),
                )
        print(f"{dataset_type.name}: complete")

    async def _export_associations(
        self,
        collections: Iterable[str],
        dataset_type: DatasetType,
        dataset_parquet_path: Path,
        output_path: Path,
    ) -> None:
        async with TemporaryDirectory() as tempdir:
            temp_path = Path(tempdir) / "association.parquet"
            await self._butler_pool.run_with_butler(
                _export_associations_sync, dataset_type, collections, temp_path
            )
            await self._duckdb_pool.run_with_duckdb(
                _filter_associations_by_matching_datasets, temp_path, dataset_parquet_path, output_path
            )


@dataclasses.dataclass
class DatasetExportResult:
    manifests: list[DatasetExportManifest]
    """Information about dataset types and the export output files."""
    collections_referenced: list[CollectionInfo]
    """List of collections referenced by the dataset search.  This includes:

    1. The resolved, flattened version of the input collections to the search.
    2. Run collections associated with datasets found via a tagged or
    calibration collection.
    """


class DatasetExportManifest(BaseModel):
    """Manifest for a single dataset type in the data release export."""

    dataset_type: SerializedDatasetType
    """Dataset type definition."""
    dataset_export_file: str
    """Path to parquet file containing the exported
    `lsst.daf.butler.DatasetRef` dataset records.
    """
    datastore_export_file: str
    """Path to parquet file containing the exported Butler datastore
    records.
    """
    association_export_file: str
    """Path to parquet file containing the association of datasets with tag and
    calibration collections.
    """


class _DatasetExportContext:
    def __init__(self) -> None:
        self.run_collections_found = set[str]()
        self.manifests = list[DatasetExportManifest]()

    def add_manifest(self, dataset_type: DatasetType) -> DatasetExportManifest:
        dataset_filename = f"{dataset_type.name}.datasets.parquet"
        datastore_filename = f"{dataset_type.name}.datastore.parquet"
        association_filename = f"{dataset_type.name}.association.parquet"
        manifest = DatasetExportManifest(
            dataset_type=dataset_type.to_simple(),
            dataset_export_file=dataset_filename,
            datastore_export_file=datastore_filename,
            association_export_file=association_filename,
        )
        self.manifests.append(manifest)
        return manifest

    def add_found_run_collections(self, collection_names: Iterable[str]) -> None:
        """Add the list of run collections found during a dataset search to the accumulated list.

        Notes
        -----
        Datasets found via tagged or calibration collections may reference run
        collections that are not present in the initial list of collections, so
        we accumulate a list of all collections encountered during export to get
        the complete set of collections related to the exported datasets.
        """
        self.run_collections_found.update(collection_names)


def _resolve_dataset_types(butler: Butler, dataset_types: Iterable[str]) -> tuple[DatasetType, ...]:
    missing_dataset_types: list[str] = []
    resolved_dataset_types = tuple(
        butler.registry.queryDatasetTypes(
            dataset_types,
            missing=missing_dataset_types,
        )
    )
    if missing_dataset_types:
        raise RuntimeError(f"Required dataset types not present in repository: {missing_dataset_types}")

    return resolved_dataset_types


def _resolve_collections(butler: Butler, collections: Iterable[str]) -> Iterable[CollectionInfo]:
    return butler.collections.query_info(
        collections, include_chains=True, flatten_chains=True, include_doc=True
    )


def _export_datasets_sync(
    butler: Butler,
    dataset_type: DatasetType,
    find_first: bool,
    collections: Iterable[str],
    output_parquet_path: Path,
) -> set[str]:
    run_collections_found: set[str] = set()
    with DatasetsParquetWriter(output_parquet_path, dataset_type) as writer, butler.query() as query:
        results = query.datasets(dataset_type, collections, find_first=find_first)
        for batch in batched(results, 50_000):
            table = DatasetRefTable.from_refs(dataset_type, batch)
            run_collections_found.update(table.get_run_collections())
            writer.add_refs_sync(table)
            print(f"{dataset_type}: {len(table)} datasets")
    return run_collections_found


def _export_associations_sync(
    butler: Butler,
    dataset_type: DatasetType,
    collections: Iterable[str],
    output_parquet_path: Path,
) -> None:
    with (
        DatasetAssociationParquetWriter(output_parquet_path, dataset_type) as writer,
        butler.query() as query,
    ):
        query = query.join_dataset_search(dataset_type, collections)
        result = query.general(
            dataset_type.dimensions,
            dataset_fields={dataset_type.name: {"dataset_id", "run", "collection", "timespan"}},
            find_first=False,
        )
        associations = DatasetAssociation.from_query_result(result, dataset_type)
        for batch in batched(associations, 50_000):
            writer.add_associations_sync(batch)


def _filter_associations_by_matching_datasets(
    conn: DuckDBPyConnection,
    association_parquet_file: Path,
    dataset_parquet_file: Path,
    output_parquet_file: Path,
) -> None:
    """Generate a new parquet file from the association parquet file, removing
    rows that do not have corresponding entries in the dataset parquet file,
    and sorting by collection name.
    """
    rel = (
        conn.from_parquet(str(association_parquet_file))
        .join(conn.from_parquet(str(dataset_parquet_file)), "dataset_id", how="semi")
        .order("collection")
    )
    schema = read_schema(association_parquet_file)
    write_duckdb_results_to_parquet(rel, output_parquet_file, schema)
