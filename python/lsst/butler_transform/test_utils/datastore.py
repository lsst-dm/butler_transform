from pathlib import Path
from uuid import uuid4

import pyarrow

from lsst.daf.butler._rubin.datastore_records import DatastoreRecordTable

from ..parquet.datastore import DatastoreParquetWriter


def make_test_datastore_parquet(output_path: Path, rows: list[dict]) -> None:
    """Write a valid datastore parquet file, given a list of dictionaries
    containing partial datastore records.
    """
    complete_rows = [
        {
            "datastore_name": "",
            "dataset_id": uuid4(),
            "path": "",
            "formatter": "",
            "storage_class": "",
            "component": None,
            "checksum": None,
            "file_size": None,
        }
        | row
        for row in rows
    ]
    table = pyarrow.Table.from_pylist(complete_rows, DatastoreRecordTable.make_arrow_schema())
    with DatastoreParquetWriter(output_path) as writer:
        writer.write_records_sync(DatastoreRecordTable.from_arrow(table))
