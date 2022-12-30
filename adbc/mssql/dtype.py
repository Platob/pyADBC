__all__ = [
    "mssql_column_to_pyarrow_field"
]

import datetime
import decimal
from typing import Optional

import pyarrow as pa
from adbc.dtype import INT32, INT16, INT8, FLOAT32, FLOAT64, STRING, int_to_timeunit
from pyarrow import field

from adbc.odbc.dtype import DATATYPES

STRING_DATATYPES = {
    "int": lambda *args, **kwargs: INT32,
    "smallint": lambda *args, **kwargs: INT16,
    "tinyint": lambda *args, **kwargs: INT8,
    "bigint": DATATYPES[int],
    "bit": DATATYPES[bool],
    "decimal": DATATYPES[decimal.Decimal],
    "numeric": DATATYPES[decimal.Decimal],
    "float": DATATYPES[float],
    "money": lambda *args, **kwargs: FLOAT64,
    "smallmoney": lambda *args, **kwargs: FLOAT32,
    "real": DATATYPES[float],
    "date": DATATYPES[datetime.date],
    "datetime": DATATYPES[datetime.datetime],
    "datetime2": DATATYPES[datetime.datetime],
    "smalldatetime": DATATYPES[datetime.datetime],
    "time": DATATYPES[datetime.time],
    "char": DATATYPES[str],
    "nchar": DATATYPES[str],
    "varchar": DATATYPES[str],
    "nvarchar": DATATYPES[str],
    "text": DATATYPES[str],
    "ntext": DATATYPES[str],
    "varbinary": DATATYPES[bytes],
    "image": DATATYPES[bytes],
    "uniqueidentifier": lambda **kwargs: STRING,
    "datetimeoffset": lambda precision=None, *args, **kwargs: pa.timestamp(int_to_timeunit(precision), "UTC")
}


def mssql_column_to_pyarrow_field(
    catalog: str, schema: str, table: str, name: str, nullable: bool, dtype: str,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    charset: Optional[str] = None
):
    try:
        return field(
            name,
            STRING_DATATYPES[dtype](precision=precision, scale=scale),
            nullable,
            {
                b"catalog": catalog,
                b"schema": schema,
                b"table": table,
                b"precision": str(precision) if precision else "",
                b"scale": str(scale) if scale else "",
                b"charset": str(charset) if charset else ""
            }
        )
    except KeyError:
        raise NotImplementedError("Unknown sql type '%s', try cast it in %s" % (
            dtype, list(STRING_DATATYPES.keys())
        ))
