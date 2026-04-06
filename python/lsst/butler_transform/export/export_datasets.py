from collections.abc import Collection, Iterable
from itertools import batched
from pathlib import Path

from anyio import (
    create_memory_object_stream,
    create_task_group,
    to_thread,
)
from anyio.abc import ObjectReceiveStream, ObjectSendStream, TaskStatus

from lsst.daf.butler import Butler, DatasetId, DatasetRef, DatasetType
from lsst.daf.butler.registry.interfaces import FakeDatasetRef

from ..parquet.datasets import DatasetsParquetWriter, read_dataset_ids
from ..parquet.datastore import ButlerDatastoreRecords, DatastoreParquetWriter
from ..utils.butler_pool import ButlerPool
from ..utils.sync_send_stream import SyncSendStream


async def export_datasets(
    butler_pool: ButlerPool,
    dataset_type: DatasetType,
    collections: Iterable[str],
    output_directory: Path,
) -> None:
    """Export datasets of the given type to two parquet files:
    1. A "datasets" file containing the same information as non-expanded
       `lsst.daf.butler.DatasetRef` instances.
    2. A "datastore" file containing the same information as
       `lsst.daf.butler.datastore.stored_file_info.StoredFileInfo`.
    """
    dataset_path = output_directory.joinpath(f"{dataset_type.name}.datasets.parquet")

    dataset_ref_send, dataset_ref_recv = create_memory_object_stream[Collection[DatasetRef]](2)

    # Export dataset refs to parquet.
    async with create_task_group() as tg:
        tg.start_soon(
            butler_pool.run_with_butler,
            lambda butler: _query_datasets(
                SyncSendStream(dataset_ref_send), butler, dataset_type, collections
            ),
        )
        tg.start_soon(_write_datasets_to_parquet, dataset_ref_recv, dataset_type, dataset_path)

    # Export datastore records to parquet.
    async with create_task_group() as tg:
        # Look up datastore records associated with the datasets.
        datastore_records_send, datastore_records_recv = create_memory_object_stream[ButlerDatastoreRecords](
            2
        )
        tg.start_soon(
            _fetch_datastore_records,
            butler_pool,
            dataset_path,
            datastore_records_send,
        )

        # Write the datastore records to parquet
        datastore_parquet_path = Path(output_directory).joinpath(f"{dataset_type.name}.datastore.parquet")
        tg.start_soon(_write_datastore_records, datastore_parquet_path, datastore_records_recv)

    print(f"{dataset_type.name}: complete")


def _query_datasets(
    output: SyncSendStream[Collection[DatasetRef]],
    butler: Butler,
    dataset_type: DatasetType,
    collections: Iterable[str],
) -> None:
    with output, butler.query() as query:
        results = query.datasets(dataset_type, collections, find_first=True)
        for batch in batched(results, 50_000):
            output.send(batch)


async def _write_datasets_to_parquet(
    input: ObjectReceiveStream[Collection[DatasetRef]],
    dataset_type: DatasetType,
    output_path: Path,
) -> None:
    async with input, DatasetsParquetWriter(output_path, dataset_type) as writer:
        async for refs in input:
            print(f"{dataset_type}: {len(refs)} datasets")
            await writer.add_refs(refs)


async def _fetch_datastore_records(
    butler_pool: ButlerPool,
    dataset_file: Path,
    output: ObjectSendStream[ButlerDatastoreRecords],
) -> None:
    async with output, create_task_group() as tg:
        async for dataset_ids in read_dataset_ids(dataset_file):
            await tg.start(_fetch_datastore_record_batch, butler_pool, dataset_ids, output)


async def _fetch_datastore_record_batch(
    butler_pool: ButlerPool,
    dataset_ids: Iterable[DatasetId],
    output: ObjectSendStream[ButlerDatastoreRecords],
    task_status: TaskStatus,
) -> None:
    refs = [FakeDatasetRef(id) for id in dataset_ids]
    async with butler_pool.get_butler() as butler:
        task_status.started()
        records = await to_thread.run_sync(butler._datastore.export_records, refs)
        await output.send(records)


async def _write_datastore_records(
    output_file: Path, input: ObjectReceiveStream[ButlerDatastoreRecords]
) -> None:
    async with input, DatastoreParquetWriter(output_file) as writer:
        async for records in input:
            await writer.write_records(records)
