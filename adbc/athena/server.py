from typing import Callable, Any, Optional
from urllib.parse import quote_plus

from boto3 import Session
from pyarrow import Schema, schema as schema_builder
from pyarrow.fs import FileSystem

from adbc.athena.dtype import dict_table_metadata_to_pyarrow_schema
from adbc.exception import TableNotFound
from adbc.filesystem import DataFileSystem
from adbc.reader import BatchReader
from adbc.server import Connection

__all__ = [
    "Athena"
]


class AthenaConnection(Connection):

    def __init__(self, server: "Athena", client):
        super().__init__(server)
        self.client = client


class Athena(DataFileSystem):

    def __init__(
        self,
        region_name: str,
        profile_name: Optional[str] = None,
        fs_builder: Callable[[Any], FileSystem] = DataFileSystem.get_s3
    ):
        super().__init__(
            fs_builder,
            path_sep="/",
            region_name=region_name,
            profile_name=profile_name
        )
        self.session = Session(profile_name=profile_name, region_name=region_name)

    def connect(self) -> AthenaConnection:
        return AthenaConnection(self, self.session.client("athena"))

    def arrow_batches(self, query: str, batch_size: int = 65536, **kwargs) -> BatchReader:
        raise NotImplementedError()

    def table_metadata(self, name: str, schema: Optional[str] = None, catalog: Optional[str] = None) -> dict:
        try:
            with self.connect() as connection:
                meta = connection.client.get_table_metadata(
                    CatalogName=catalog,
                    DatabaseName=schema,
                    TableName=name
                )["TableMetadata"]
                meta["catalog"] = catalog
                meta["database"] = schema
                return meta
        except Exception as e:
            if "EntityNotFoundException" in str(e):
                raise TableNotFound("%s: Table '%s'" % (repr(self), name))
            else:
                raise e

    def table_schema(self, name: str, schema: Optional[str] = None, catalog: Optional[str] = None) -> Schema:
        meta = self.table_metadata(name, schema, catalog)
        return dict_table_metadata_to_pyarrow_schema(meta["catalog"], meta["database"], meta)

    def write(
        self,
        table: str,
        schema: Optional[str] = None,
        catalog: Optional[str] = None,
        partition_values: Optional[dict[str, str]] = None,
        **kwargs
    ):
        meta = self.table_metadata(table, schema, catalog)
        parameters = meta["Parameters"]
        file_format = parameters["classification"]
        base_dir = parameters["location"][5:]
        partition_by = [d["Name"] for d in meta.get("PartitionKeys", [])]
        schema_arrow = dict_table_metadata_to_pyarrow_schema(meta["catalog"], meta["database"], meta)

        if partition_values:
            base_dir += self.path_sep + self.path_sep.join((
                "%s=%s" % (k, quote_plus(v))
                for k, v in partition_values.items()
            ))
            partition_by = [name for name in partition_by if name not in partition_values.keys()]
            schema_arrow = schema_builder(
                [f for f in schema_arrow if f.name not in partition_values.keys()],
                schema_arrow.metadata
            )

        return super(Athena, self).write(
            table,
            base_dir,
            file_format,
            None,
            partition_by,
            schema,
            catalog,
            schema_arrow
        )
