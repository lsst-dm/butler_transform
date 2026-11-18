import click

from lsst.daf.butler import Butler, CollectionType


@click.command()
@click.argument("backup_repo")
@click.argument("target_repo")
@click.argument("dataset_file")
def fix_associations(backup_repo: str, target_repo: str, dataset_file: str) -> None:
    with (
        Butler.from_config(backup_repo) as backup_butler,
        # Butler.from_config(target_repo, writeable=True) as target_butler,
    ):
        all_tagged_collections = backup_butler.collections.query_info(
            "*", collection_types=CollectionType.TAGGED, include_summary=True
        )
        collections = [c.name for c in all_tagged_collections if "raw" in c.dataset_types]
        print(collections)


if __name__ == "__main__":
    fix_associations()
