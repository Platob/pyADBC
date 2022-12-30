from typing import Optional

from arrow_odbc import insert_into_table

from adbc.exception import TableNotFound
from adbc.reader import BatchReader
from adbc.writer.batchwriter import BatchWriter


class ODBCWriter(BatchWriter):

    def __init__(self, server: "ODBC", table: str, schema: Optional[str] = None, catalog: Optional[str] = None):
        self.server = server
        self.table = table
        self.schema = schema
        self.catalog = catalog

    def write_batches(
        self,
        batches: BatchReader,
        chunk_size: int = 65536,
        cast: bool = True,
        safe: bool = True,
        append: bool = True,
        **kwargs
    ):
        if cast:
            try:
                table_schema = self.server.table_schema(self.table, schema=self.schema, catalog=self.catalog)
            except TableNotFound as e:
                # TODO should create table from schema
                raise e
            batches = batches.cast(table_schema, safe=safe, fill_empty=False, drop=True)
        insert_into_table(
            batches,
            chunk_size,
            self.table,
            self.server.uri,
            **kwargs
        )
