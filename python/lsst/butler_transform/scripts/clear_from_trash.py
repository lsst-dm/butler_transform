import asyncio

import click

from lsst.daf.butler import Butler
from lsst.daf.butler.datastore.stored_file_info import StoredFileInfo
from lsst.daf.butler.datastores.fileDatastore import FileDatastore

from ..parquet.datasets import read_dataset_ids


@click.command()
@click.argument("target_repo")
@click.argument("dataset_file")
def clear_from_trash(target_repo: str, dataset_file: str):
    with (
        Butler.from_config(target_repo, writeable=True) as target_butler,
    ):

        async def doEmpty():
            datastore = target_butler._datastore
            assert isinstance(datastore, FileDatastore)
            async for ids in read_dataset_ids(dataset_file):
                if len(ids) > 0:
                    with datastore.bridge.emptyTrash(
                        datastore._table,
                        record_class=StoredFileInfo,
                        record_column="path",
                        selected_ids=ids,
                    ) as trash_data:
                        print(len(trash_data))

        asyncio.run(doEmpty())


if __name__ == "__main__":
    clear_from_trash()
