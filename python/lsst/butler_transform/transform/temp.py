import asyncio
from glob import glob

from ..parquet.datastore import DatastoreParquetReader
from .rewrite_datastore_paths import make_uris_absolute, map_uris_to_datastores, rewrite_datastore_and_path


def remap_paths(rows) -> None:
    original_datastores = {"FileDatastore@<butlerRoot>": "file:///sdf/group/rubin/repo/dp2_prep/"}
    make_uris_absolute(rows, original_datastores)
    map = {
        "file:///sdf/group/rubin/repo/dp2_prep/": "dp2",
        # This is a rucio alias for /sdf/data/rubin/repo/main_20210215/LSSTCam/calib
        "file:///sdf/data/rubin/rses/lsst/butlerdisk/rucio/repo/ancillary/LSSTCam/calib": "calib",
        "file:///sdf/data/rubin/shared/refcats/": "refcats",
        "file:///sdf/data/rubin/lsstdata/offline/instrument/": "raw",
    }
    map_uris_to_datastores(rows, datastore_map=map)


async def do_thing() -> None:
    root_dir = "/Users/david.irving/dp2-export"

    for file in glob("*.datastore.parquet", root_dir=root_dir):
        print(file)
        async with DatastoreParquetReader(f"{root_dir}/{file}") as reader:
            async for batch in reader.read():
                rewrite_datastore_and_path(batch, remap_paths)


asyncio.run(do_thing())
