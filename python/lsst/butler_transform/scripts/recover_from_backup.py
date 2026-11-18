import asyncio
from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Self

import click
from anyio import AsyncContextManagerMixin, open_file
from pydantic import BaseModel

from lsst.daf.butler import Butler

from ..export.export_datasets import DatasetExporter, DatasetExportManifest
from ..utils.butler_process_pool import ButlerProcessPool
from ..utils.duckdb import DuckDbPool


@click.command()
@click.argument("backup_repo")
@click.argument("target_repo")
@click.argument("collection")
@click.argument("output_directory", default="dataset_recovery")
def find_missing_datasets(backup_repo: str, target_repo: str, collection: str, output_directory: str) -> None:
    output_path = Path(output_directory)

    async def do_recover():
        async with BackupRecovery(backup_repo, target_repo) as recovery:
            await recovery.export_dataset_lists(collection, output_path)

    asyncio.run(do_recover())


class BackupRecovery(AsyncContextManagerMixin):
    def __init__(self, backup_repo: str, target_repo: str) -> None:
        self._backup_repo = backup_repo
        self._target_repo = target_repo
        self._max_concurrency = 16

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
        backup_output_path = output_path / "backup"
        backup_output_path.mkdir(exist_ok=True, parents=True)
        target_output_path = output_path / "target"
        target_output_path.mkdir(exist_ok=True, parents=True)
        async with asyncio.TaskGroup() as tg:
            backup_task = tg.create_task(
                self._export_collection(self._backup_butler, dataset_types, collection, backup_output_path)
            )
            target_task = tg.create_task(
                self._export_collection(self._target_butler, dataset_types, collection, target_output_path)
            )

        manifest = BackupExportManifest(
            backup_datasets=backup_task.result(), target_datasets=target_task.result()
        )
        async with await open_file(output_path.joinpath("manifest.json"), "wb") as fh:
            await fh.write(manifest.model_dump_json(indent=2).encode("utf-8"))

    async def _export_collection(
        self, butler_pool: ButlerProcessPool, dataset_types: Iterable[str], collection: str, output_path
    ):
        return await DatasetExporter(
            butler_pool,
            self._duckdb_pool,
            output_directory=output_path,
            include_datastore=False,
            include_associations=False,
            find_first=False,
        ).export(dataset_types, [], [collection])


def _get_dataset_types(butler: Butler, collection: str) -> frozenset[str]:
    dataset_types = butler.collections.get_info(collection, include_summary=True).dataset_types
    assert dataset_types is not None
    return dataset_types


class BackupExportManifest(BaseModel):
    backup_datasets: list[DatasetExportManifest]
    target_datasets: list[DatasetExportManifest]


if __name__ == "__main__":
    find_missing_datasets()
