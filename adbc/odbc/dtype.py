__all__ = [
    "pyodbc_description_to_pyarrow_field",
    "DATATYPES"
]

import decimal
import datetime
from typing import Optional

from adbc.dtype import BINARY, UTCTIMESTAMP, TIMESTAMP, TIMESTAMPMS, STRING, LARGE_STRING, BOOL, fine_float, fine_int, \
    DATE, TIMETYPES, fine_decimal, int_to_timeunit, LARGE_BINARY
import pyarrow as pa
from pyarrow import Field, field


def pyodbc_string(precision=None, scale=None, *args, **kwargs):
    if precision:
        if scale:
            if precision == 34 and scale == 7:
                # DATETIMEOFFSET
                return UTCTIMESTAMP
            elif precision == 27 and scale == 7:
                # DATETIME2
                return TIMESTAMP
            elif precision == 23 and scale == 3:
                # DATETIME
                return TIMESTAMPMS
        if 0 < precision <= 42000:
            return STRING
    return LARGE_STRING


DATATYPES = {
    bytes: lambda precision=None, *args, **kwargs:
        BINARY if precision is not None and 0 < int(precision) <= 42000 else LARGE_BINARY,
    bytearray: lambda precision=None, *args, **kwargs:
        BINARY if precision is not None and 0 < int(precision) <= 42000 else LARGE_BINARY,
    str: pyodbc_string,
    bool: lambda *args, **kwargs: BOOL,
    float: lambda precision=32, *args, **kwargs: fine_float(precision),
    int: lambda precision, *args, **kwargs: fine_int(precision),
    decimal.Decimal: lambda precision=38, scale=18, *args, **kwargs: fine_decimal(precision, scale),
    datetime.date: lambda *args, **kwargs: DATE,
    datetime.time: lambda scale=9, *args, **kwargs: TIMETYPES[int_to_timeunit(scale)],
    datetime.datetime: lambda scale=None, *args, **kwargs: pa.timestamp(int_to_timeunit(scale))
}


def pyodbc_description_to_pyarrow_field(description: tuple, metadata: Optional[dict] = None) -> Field:
    """
    Parse input cursor.description
    (name, type_code, display_size, internal_size, precision, scale, null_ok)
    Example: ('string', <class 'str'>, None, 64, 64, 0, False)
    :rtype: Field
    """
    name, type_code, _, _, precision, scale, null_ok = description
    return field(
        name,
        DATATYPES[type_code](precision=precision, scale=scale),
        null_ok,
        metadata
    )
