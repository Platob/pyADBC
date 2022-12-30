from arrow_odbc import read_arrow_batches_from_odbc

from adbc.odbc.writer import ODBCWriter
from adbc.reader import LazyReader
from adbc.reader.batchreader import BatchReader
from adbc.server import Server, Connection
from typing import Optional, List

__all__ = [
    "ODBC"
]


class ODBCConnection(Connection):

    def __init__(self, server: "ODBC", client):
        super().__init__(server)
        self.client = client

    def close(self):
        del self.client
        super(ODBCConnection, self).close()


class ODBC(Server):

    def __init__(self, protocol: str, uri: str):
        super(ODBC, self).__init__(protocol=protocol)
        self.uri = uri

    def connect(self):
        from pyodbc import connect
        return ODBCConnection(self, connect(self.uri))

    def arrow_batches(
        self,
        query: str,
        batch_size: int = 65536,
        user: Optional[str] = None,
        password: Optional[str] = None,
        parameters: Optional[List[Optional[str]]] = None,
        max_text_size: Optional[int] = None,
        max_binary_size: Optional[int] = None,
        falliable_allocations: bool = True,
        lazy: bool = False
    ):
        reader = read_arrow_batches_from_odbc(
            query,
            batch_size,
            self.uri,
            user,
            password,
            parameters,
            max_text_size,
            max_binary_size,
            falliable_allocations
        )
        return LazyReader(
            read_arrow_batches_from_odbc,
            reader.schema,
            False,
            query,
            batch_size,
            self.uri,
            user,
            password,
            parameters,
            max_text_size,
            max_binary_size,
            falliable_allocations
        ) if lazy else BatchReader(reader.schema, reader)

    def write(self, table: str, schema: Optional[str] = None, catalog: Optional[str] = None):
        return ODBCWriter(self, table, schema, catalog)
