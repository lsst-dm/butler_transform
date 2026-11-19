import click

from lsst.daf.butler import Butler, CollectionType

from ..parquet.datasets import read_dataset_ids


@click.command()
@click.argument("backup_repo")
@click.argument("target_repo")
@click.argument("dataset_file")
def fix_associations(backup_repo: str, target_repo: str, dataset_file: str) -> None:
    dataset_type = "raw"
    with (
        Butler.from_config(backup_repo) as backup_butler,
        # Butler.from_config(target_repo, writeable=True) as target_butler,
    ):
        all_tagged_collections = backup_butler.collections.query_info(
            "*", collection_types=CollectionType.TAGGED, include_summary=True
        )
        collections = [c.name for c in all_tagged_collections if dataset_type in c.dataset_types]

        async def xfer():
            async for ids in read_dataset_ids(dataset_file):
                with backup_butler.query() as query:
                    x = query.expression_factory
                    query = (
                        query.join_dataset_search("raw", collections=collections)
                        .where(x["dataset_type"].dataset_id.in_iterable(ids))
                        .general(
                            [], dataset_fields={dataset_type: {"dataset_id", "collection"}}, find_first=False
                        )
                    )
                    for row in query:
                        print(row)

        print(collections)


if __name__ == "__main__":
    fix_associations()
