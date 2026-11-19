import asyncio

import click

from lsst.daf.butler import Butler
from lsst.daf.butler._rubin.transfer_datasets_in_place import transfer_datasets_in_place

from ..parquet.datasets import read_dataset_ids


@click.command()
@click.argument("backup_repo")
@click.argument("target_repo")
@click.argument("dataset_file")
def do_transfer(backup_repo: str, target_repo: str, dataset_file: str):
    with (
        Butler.from_config(backup_repo) as backup_butler,
        Butler.from_config(target_repo, writeable=True) as target_butler,
    ):

        async def xfer():
            async for ids in read_dataset_ids(dataset_file):
                refs = backup_butler.get_many_datasets(ids)
                if len(refs) != len(ids):
                    raise RuntimeError("Datasets are missing from source repository")
                transfer_datasets_in_place(backup_butler, target_butler, refs)
                print(len(refs))

        asyncio.run(xfer())


if __name__ == "__main__":
    do_transfer()
