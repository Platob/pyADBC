from typing import Optional

import pyarrow as pa
from pyarrow import field, DataType, Field, Schema

from adbc.dtype import TIMETYPES, STRING

__all__ = [
    "dict_table_metadata_to_pyarrow_schema",
    "dict_to_pyarrow_field",
    "sqltype_to_datatype",
    "query_result_column_to_pyarrow_field"
]


def fine_decimal(precision: int, scale: int):
    if precision > 38:
        return pa.decimal256(precision, scale)
    else:
        return pa.decimal128(precision, scale)


def int_to_timeunit(i: int) -> str:
    if i == 0:
        return "s"
    elif i <= 3:
        return "ms"
    elif i <= 6:
        return "us"
    return "ns"


DATATYPES = {
    "string": lambda precision=None, *args, **kwargs:
        pa.large_string() if precision is not None and precision > 42000 else pa.string(),
    "date": lambda unit, tz, **kwargs: pa.date32(),
    "timestamp": lambda unit, tz, precision=None, **kwarg:
        pa.timestamp(int_to_timeunit(precision), tz) if precision else pa.timestamp(unit, tz),
    "int": lambda *args, **kwargs: pa.int32(),
    "integer": lambda *args, **kwargs: pa.int32(),
    "tinyint": lambda *args, **kwargs: pa.int8(),
    "smallint": lambda *args, **kwargs: pa.int16(),
    "bigint": lambda *args, **kwargs: pa.int64(),
    "boolean": lambda *args, **kwargs: pa.bool_(),
    "decimal": lambda precision, scale, **kwargs: fine_decimal(precision, scale),
    "double": lambda *args, **kwargs: pa.float64(),
    "float": lambda *args, **kwargs: pa.float32(),
    "binary": lambda precision=None, **kwargs:
        pa.large_binary() if precision is not None and precision > 42000 else pa.binary(),
    "char": lambda precision=None, *args, **kwargs:
        pa.large_string() if precision is not None and precision > 42000 else pa.string(),
    "varchar": lambda precision=None, *args, **kwargs:
        pa.large_string() if precision is not None and precision > 42000 else pa.string(),
    "time": lambda precision=9, *args, **kwargs: TIMETYPES[int_to_timeunit(precision)],
    "timestamp with time zone": lambda **kwargs: pa.string()
}


def sqltype_to_datatype(
    sqltype: str,
    unit: str = "us",
    tz: Optional[str] = "UTC",
    **kwargs
) -> DataType:
    try:
        if '(' in sqltype:
            key, args = sqltype.split("(", 1)
            return DATATYPES[key](*(int(_) for _ in args[:-1].split(",")))
        else:
            return DATATYPES[sqltype](unit=unit, tz=tz, **kwargs)
    except KeyError as e:
        print(e)
        return STRING


def dict_to_pyarrow_field(meta: dict, nullable: bool = True):
    return field(
        meta["Name"],
        sqltype_to_datatype(meta["Type"]),
        nullable,
        {k: v for k, v in meta.items() if k != "Name"}
    )


def dict_table_metadata_to_pyarrow_schema(
    catalog: str,
    database: str,
    meta: dict
) -> Schema:
    """
    Parse dict returned from boto3.client to owlna.Table
    :param catalog: Athena catalog name
    :param database: Athena database name
    :param meta: dict returned by boto3.client.get_table_metadata
    :rtype Schema: pyarrow.Schema
    """
    return pa.schema(
        [
            *[dict_to_pyarrow_field(_, nullable=False) for _ in meta["PartitionKeys"]],
            *[dict_to_pyarrow_field(_, nullable=True) for _ in meta["Columns"]]
        ],
        metadata={
            "protocol": "athena",
            "catalog": catalog,
            "database": database
        }
    )


def query_result_column_to_pyarrow_field(meta: dict) -> Field:
    return field(
        meta["Name"],
        sqltype_to_datatype(
            meta["Type"], precision=meta["Precision"], scale=meta["Scale"], tz=None
        ),
        nullable=not meta["Nullable"].startswith("T"),  # = 'UNKNOWN' atm
        metadata={
            k: str(v) for k, v in meta.items() if k not in {"Name", "Nullable"}
        }
    )
