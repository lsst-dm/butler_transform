import asyncio
from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Self

import click
from anyio import AsyncContextManagerMixin, open_file
from duckdb import DuckDBPyConnection
from pyarrow.parquet import read_schema
from pydantic import BaseModel

from lsst.daf.butler import Butler

from ..export.export_datasets import DatasetExporter, DatasetExportManifest
from ..utils.butler_process_pool import ButlerProcessPool
from ..utils.duckdb import DuckDbPool, write_duckdb_results_to_parquet


@click.command()
@click.argument("backup_repo")
@click.argument("target_repo")
@click.argument("collection")
@click.argument("output_directory", default="dataset_recovery")
def find_missing_datasets(backup_repo: str, target_repo: str, collection: str, output_directory: str) -> None:
    async def do_recover():
        async with BackupRecovery(backup_repo, target_repo, output_directory) as recovery:
            # await recovery.export_dataset_lists(collection)
            await recovery.diff_dataset_lists()

    asyncio.run(do_recover())


class BackupRecovery(AsyncContextManagerMixin):
    def __init__(self, backup_repo: str, target_repo: str, output_directory: str) -> None:
        self._max_concurrency = 16
        self._backup_repo = backup_repo
        self._target_repo = target_repo
        output_path = Path(output_directory)
        self._backup_output_path = output_path / "backup"
        self._target_output_path = output_path / "target"
        self._diff_output_path = output_path / "diff"
        for path in (self._backup_output_path, self._target_output_path, self._diff_output_path):
            path.mkdir(exist_ok=True, parents=True)

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        async with (
            ButlerProcessPool.from_config(self._backup_repo, self._max_concurrency) as backup_butler,
            ButlerProcessPool.from_config(self._target_repo, self._max_concurrency) as target_butler,
            DuckDbPool.initialize() as duckdb_pool,
        ):
            self._backup_butler = backup_butler
            self._target_butler = target_butler
            self._duckdb_pool = duckdb_pool
            yield self

    async def export_dataset_lists(self, collection: str, output_path: Path) -> None:
        dataset_types = await self._target_butler.run_with_butler(_get_dataset_types, collection)
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                self._export_collection(
                    self._backup_butler, dataset_types, collection, self._backup_output_path
                )
            )
            tg.create_task(
                self._export_collection(
                    self._target_butler, dataset_types, collection, self._target_output_path
                )
            )

    async def _export_collection(
        self, butler_pool: ButlerProcessPool, dataset_types: Iterable[str], collection: str, output_path: Path
    ):
        result = await DatasetExporter(
            butler_pool,
            self._duckdb_pool,
            output_directory=output_path,
            include_datastore=False,
            include_associations=False,
            find_first=False,
        ).export(dataset_types, [], [collection])

        manifest = BackupExportManifest(datasets=result.manifests)
        async with await open_file(output_path.joinpath("manifest.json"), "wb") as fh:
            await fh.write(manifest.model_dump_json(indent=2).encode("utf-8"))

    async def diff_dataset_lists(self) -> None:
        target_manifest = _read_manifest(self._target_output_path)
        async with asyncio.TaskGroup() as tg:
            for manifest in target_manifest.datasets:
                filename = manifest.dataset_export_file
                tg.create_task(
                    self._duckdb_pool.run_with_duckdb(
                        _generate_missing_datasets_parquet,
                        self._backup_output_path / filename,
                        self._target_output_path / filename,
                        self._diff_output_path / filename,
                    )
                )


def _get_dataset_types(butler: Butler, collection: str) -> frozenset[str]:
    dataset_types = butler.collections.get_info(collection, include_summary=True).dataset_types
    assert dataset_types is not None
    return dataset_types


def _generate_missing_datasets_parquet(
    conn: DuckDBPyConnection,
    backup_parquet_file: Path,
    target_parquet_file: Path,
    output_parquet_file: Path,
) -> None:
    rel = conn.from_parquet(str(backup_parquet_file)).join(
        conn.from_parquet(str(target_parquet_file)), "dataset_id", how="anti"
    )
    schema = read_schema(backup_parquet_file)
    write_duckdb_results_to_parquet(rel, output_parquet_file, schema)


class BackupExportManifest(BaseModel):
    datasets: list[DatasetExportManifest]


def _read_manifest(dataset_directory: Path) -> BackupExportManifest:
    manifest_file = dataset_directory / "manifest.json"
    return BackupExportManifest.model_validate_json(manifest_file.read_text())


if __name__ == "__main__":
    find_missing_datasets()
