from abc import abstractmethod

__all__ = [
    "Connection", "Server"
]

from typing import Optional

from pyarrow import Schema

from adbc.reader import BatchReader


class Connection:

    def __init__(self, server):
        self.server = server

        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if not self.closed:
            self.closed = True


class Server:

    def __init__(self, protocol: str):
        self.protocol = protocol

    @abstractmethod
    def connect(self) -> Connection:
        raise NotImplementedError(f"{self}.connect not implemented")

    @abstractmethod
    def table_schema(self, name: str, schema: Optional[str] = None, catalog: Optional[str] = None) -> Schema:
        raise NotImplementedError(f"{self}.table_schema not implemented")

    @abstractmethod
    def arrow_batches(
        self,
        query: str,
        batch_size: int = 65536,
        **kwargs
    ) -> BatchReader:
        raise NotImplementedError()
