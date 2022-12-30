from typing import Optional

import pyarrow
from pyarrow import schema as schema_builder, Schema

from adbc.enums import Protocol
from adbc.exception import TableNotFound
from adbc.mssql.dtype import mssql_column_to_pyarrow_field
from adbc.odbc import ODBC

__all__ = [
    "MSSQL"
]


class MSSQL(ODBC):

    def __init__(self, uri: str):
        super(MSSQL, self).__init__(protocol=Protocol.mssql, uri=uri)

    def table_schema(self, name: str, schema: Optional[str] = None, catalog: Optional[str] = None) -> Schema:
        fields: pyarrow.Table = self.arrow_batches(
            "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, convert(bit, case when IS_NULLABLE = 'YES' "
            "then 1 else 0 end) as NULLABLE, DATA_TYPE, case when DATETIME_PRECISION is not null then "
            "DATETIME_PRECISION when (CHARACTER_MAXIMUM_LENGTH is not null and CHARACTER_MAXIMUM_LENGTH != -1) then "
            "CHARACTER_MAXIMUM_LENGTH else NUMERIC_PRECISION end as PRECISION, NUMERIC_SCALE as SCALE, "
            "CHARACTER_SET_NAME as CHARSET FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'" % name,
            100
        ).read_all()

        fields: dict = fields.to_pydict()
        fields: list = [
            mssql_column_to_pyarrow_field(
                catalog, schema, table, name, nullable, dtype, precision, scale, charset
            )
            for catalog, schema, table, name, nullable, dtype, precision, scale, charset
            in zip(*(fields[k] for k in fields))
        ]
        if fields:
            return schema_builder(fields, {
                k: fields[0].metadata[k]
                for k in (b"catalog", b"schema", b"table")
            })
        raise TableNotFound("%s: Table '%s' not found" % (repr(self), name))
