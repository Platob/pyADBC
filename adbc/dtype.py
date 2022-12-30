import datetime
import decimal
import re
from typing import Union, Optional, Any

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import RecordBatch, Schema, schema as schema_builder, Field, field as field_builder, Array, Decimal128Type, \
    Decimal256Type, TimestampType, ArrowInvalid, Table, array, Time32Type, DataType

STRING = UTF8 = pa.string()
LARGE_STRING = pa.large_string()
BOOL = BOOLEAN = pa.bool_()
INT = INT32 = pa.int32()
INT8, INT16, INT64 = pa.int8(), pa.int16(), pa.int64()
UINT = UINT32 = pa.uint32()
UINT8, UINT16, UINT64 = pa.uint8(), pa.uint16(), pa.uint64()
FLOAT16, FLOAT32, FLOAT64 = pa.float16(), pa.float32(), pa.float64()
DOUBLE = FLOAT64
DATE = DATE32 = pa.date32()
DATE64 = pa.date64()
DATETIME = TIMESTAMP = pa.timestamp("ns")
UTCTIMESTAMP = pa.timestamp("ns", "UTC")
TIMESTAMPMS = pa.timestamp("ms")
TIME = TIME64 = TIMENS = pa.time64("ns")
TIMEUS = pa.time64("us")
TIMEMS = TIME32 = pa.time32("ms")
TIMES = pa.time32("s")
TIMETYPES = {
    "s": TIMES, "ms": TIMEMS, "us": TIMEUS, "ns": TIMENS
}
BINARY = pa.binary(-1)
LARGE_BINARY = pa.large_binary()
NULL = pa.null()


def return_iso(o):
    return o


def int_to_timeunit(i: Optional[int] = None) -> str:
    if i is not None:
        if i == 0:
            return "s"
        elif i <= 3:
            return "ms"
        elif i <= 6:
            return "us"
    return "ns"


def fine_int(precision: int):
    if precision <= 3:
        return INT8
    if precision <= 5:
        return INT16
    if precision <= 10:
        return INT32
    return INT64


def fine_float(precision: int):
    return FLOAT32 if precision < 25 else FLOAT64


def fine_decimal(precision: int, scale: int):
    if precision > 38:
        return pa.decimal256(precision, scale)
    else:
        return pa.decimal128(precision, scale)


DATATYPES = {
    bytes: lambda precision=None, *args, **kwargs:
        BINARY if precision is not None and 0 < int(precision) <= 42000 else LARGE_BINARY,
    bytearray: lambda precision=None, *args, **kwargs:
        BINARY if precision is not None and 0 < int(precision) <= 42000 else LARGE_BINARY,
    str: lambda precision=None, *args, **kwargs:
        STRING if precision is not None and 0 < int(precision) <= 42000 else LARGE_STRING,
    bool: lambda *args, **kwargs: BOOL,
    float: lambda precision=32, *args, **kwargs: fine_float(precision),
    int: lambda precision=12, *args, **kwargs: fine_int(precision),
    decimal.Decimal: lambda precision=38, scale=18, *args, **kwargs: fine_decimal(precision, scale),
    datetime.date: lambda *args, **kwargs: DATE,
    datetime.time: lambda scale=9, *args, **kwargs: TIMETYPES[int_to_timeunit(scale)],
    datetime.datetime: lambda scale=None, *args, **kwargs: pa.timestamp(int_to_timeunit(scale))
}


STRING_DATATYPES = {
    "string": DATATYPES[str],
    "date": DATATYPES[datetime.date],
    "timestamp": lambda unit, tz, precision=None, **kwarg:
        pa.timestamp(int_to_timeunit(precision), tz) if precision else pa.timestamp(unit, tz),
    "int": lambda *args, **kwargs: INT32,
    "integer": lambda *args, **kwargs: INT32,
    "tinyint": lambda *args, **kwargs: INT8,
    "smallint": lambda *args, **kwargs: INT16,
    "bigint": lambda *args, **kwargs: INT64,
    "boolean": lambda *args, **kwargs: BOOL,
    "decimal": lambda precision, scale, **kwargs: fine_decimal(precision, scale),
    "numeric": lambda precision, scale, **kwargs: fine_decimal(precision, scale),
    "double": lambda *args, **kwargs: FLOAT64,
    "float": lambda *args, **kwargs: FLOAT32,
    "real": lambda *args, **kwargs: FLOAT32,
    "binary": lambda precision=None, **kwargs:
        BINARY if precision is not None and 0 < int(precision) <= 42000 else LARGE_BINARY,
    "char": lambda precision=None, *args, **kwargs:
        STRING if precision is not None and 0 < int(precision) <= 42000 else LARGE_STRING,
    "varchar": lambda precision=None, *args, **kwargs:
        STRING if precision is not None and 0 < int(precision) <= 42000 else LARGE_STRING,
    "time": lambda precision=9, *args, **kwargs: TIMETYPES[int_to_timeunit(precision)]
}


ALL_DATATYPES = {
    **DATATYPES,
    **STRING_DATATYPES
}


def get_field(
    schema: Schema,
    name: str,
    raise_error: bool = True,
    replace_name: bool = False
) -> Optional[Field]:
    try:
        idx = schema.names.index(name)
        return schema.field(idx)
    except ValueError:
        # b.schema.names.index("name")
        # ValueError: 'name' is not in list
        for batch_field in schema:
            if batch_field.name.lower() == name.lower():
                return field_builder(
                    name, batch_field.type, batch_field.nullable, batch_field.metadata
                ) if replace_name else batch_field
        if raise_error:
            raise KeyError("Cannot find Field<'%s'> in schema %s" % (
                name, schema.names
            ))
        return None


def intersect_schemas(schema: Schema, other: Schema, replace_name: bool = False):
    fields = [get_field(schema, name, False, replace_name) for name in other.names]
    return schema_builder(
        [_ for _ in fields if _ is not None],
        schema.metadata
    )


def get_batch_column_or_empty(
    batch: Union[RecordBatch, Table], field: Field,
    fill_empty: bool = True,
    drop: bool = False
) -> (Field, Array):
    try:
        idx = batch.schema.names.index(field.name)
        return batch.schema.field(idx), batch.column(idx)
    except ValueError:
        # b.schema.names.index("name")
        # ValueError: 'name' is not in list
        data, idx = None, -1

        for batch_field in batch.schema:
            idx += 1

            if batch_field.name.lower() == field.name.lower():
                data = field_builder(field.name, batch_field.type, batch_field.nullable, batch_field.metadata), batch.column(idx)
                break

        if data:
            return data
        elif field.nullable and fill_empty:
            return field, pa.array([None] * batch.num_rows, field.type)
        elif drop:
            return None
        else:
            raise KeyError("Cannot find Field<'%s', %s, nullable=%s> in batch columns %s, or fill with nulls" % (
                field.name, field.type, field.nullable, batch.schema.names
            ))


def string_to_timestamp(arr: Array, dtype: TimestampType, safe: bool = True, **kwargs):
    try:
        return arr.cast(dtype, safe)
    except ArrowInvalid as e:
        if safe:
            raise e
        else:
            import pandas

            return timestamp_to_timestamp(
                Array.from_pandas(pandas.to_datetime(arr.to_pandas())),
                dtype,
                safe,
                **kwargs
            )


def string_to_date(arr: Array, safe: bool = True, **kwargs):
    if safe:
        return array(
            [None if _ is None else datetime.date.fromisoformat(_) for _ in (_.as_py() for _ in arr)],
            DATE, safe=safe
        )
    return string_to_timestamp(arr, TIMESTAMP, safe=safe).cast(DATE, safe)


def string_to_time(arr: Array, dtype: Time32Type, safe: bool = True, **kwargs):
    try:
        unit = dtype.unit
    except AttributeError:
        unit = re.findall(r"\[(.*?)\]", str(dtype))[0]
    return string_to_timestamp(arr, pa.timestamp(unit), safe=safe).cast(dtype, safe)


def timestamp_to_timestamp(arr: Array, dtype: TimestampType, safe: bool = True, **kwargs):
    if arr.type.tz is None:
        # naive
        if dtype.tz in {"UTC", "GMT"} or dtype.tz == arr.type.tz:
            return arr.cast(dtype, safe)
        else:
            # need assume timezone
            # return Array.from_pandas(
            #     arr.to_pandas().dt.tz_localize(dtype.tz),
            #     safe=safe
            # ).cast(dtype, safe)
            return pc.assume_timezone(
                arr,
                dtype.tz,
                ambiguous="raise" if safe else "earliest",
                nonexistent="raise" if safe else "earliest"
            ) if arr.type.unit == dtype.unit else pc.assume_timezone(
                arr,
                dtype.tz,
                ambiguous="raise" if safe else "earliest",
                nonexistent="raise" if safe else "earliest"
            ).cast(dtype, safe)
    else:
        return arr.cast(dtype, safe)


TYPE_CASTS = {
    (STRING, INT8): lambda arr, safe=True, **kwargs:
        pc.round(arr.cast(FLOAT32, safe=safe)).cast(INT8, safe=safe),
    (STRING, INT16): lambda arr, safe=True, **kwargs:
        pc.round(arr.cast(FLOAT32, safe=safe)).cast(INT16, safe=safe),
    (STRING, INT32): lambda arr, safe=True, **kwargs:
        pc.round(arr.cast(FLOAT64, safe=safe)).cast(INT32, safe=safe),
    (STRING, INT64): lambda arr, safe=True, **kwargs:
        pc.round(arr.cast(FLOAT64, safe=safe)).cast(INT64, safe=safe),
    (STRING, Decimal128Type): lambda arr, dtype, safe=True, **kwargs:
        arr.cast(FLOAT64, safe=safe).cast(dtype, safe=safe),
    (STRING, Decimal256Type): lambda arr, dtype, safe=True, **kwargs:
        arr.cast(FLOAT64, safe=safe).cast(dtype, safe=safe),
    (STRING, TimestampType): string_to_timestamp,
    (STRING, DATE): string_to_date,
    (STRING, TIMES): string_to_time,
    (STRING, TIMEMS): string_to_time,
    (STRING, TIMEUS): string_to_time,
    (STRING, TIMENS): string_to_time,
    (TimestampType, TimestampType): timestamp_to_timestamp
}


def cast_array(array: Array, field: Union[Field, DataType], safe: bool = True):
    try:
        dtype = field.type if isinstance(field, Field) else field
        if array.type.equals(dtype):
            return array
        elif (array.type, dtype) in TYPE_CASTS:
            return TYPE_CASTS[(array.type, dtype)](array, safe=safe, dtype=dtype)
        elif (array.type.__class__, dtype) in TYPE_CASTS:
            return TYPE_CASTS[(array.type.__class__, dtype)](array, safe=safe, dtype=dtype)
        elif (array.type, dtype.__class__) in TYPE_CASTS:
            return TYPE_CASTS[(array.type, dtype.__class__)](array, safe=safe, dtype=dtype)
        elif (array.type.__class__, dtype.__class__) in TYPE_CASTS:
            return TYPE_CASTS[(array.type.__class__, dtype.__class__)](array, safe=safe, dtype=dtype)
        else:
            return array.cast(dtype, safe=safe)
    except Exception as e:
        raise ArrowInvalid("Cannot cast to %s, safe=%s: %s" % (
            field, safe, e
        ))


def cast_batch(
    batch: Union[RecordBatch, Table], schema: Schema,
    safe: bool = True,
    fill_empty: bool = True,
    drop: bool = False
) -> Union[RecordBatch, Table]:
    # check names
    if batch.schema.names != schema.names:
        columns: list[(Field, Array)] = [get_batch_column_or_empty(batch, field, fill_empty, drop) for field in schema]

        if drop:
            columns = [c for c in columns if c is not None]
            schema = schema_builder([_[0] for _ in columns], schema.metadata)

        return cast_batch(
            batch.__class__.from_arrays(
                [_[1] for _ in columns],
                schema=schema_builder([_[0] for _ in columns])
            ),
            schema=schema,
            safe=safe,
            fill_empty=fill_empty,
            drop=drop
        )

    if batch.schema == schema:
        return batch.replace_schema_metadata(schema.metadata)
    else:
        # check data types and cast
        return batch.__class__.from_arrays(
            [
                cast_array(batch.column(i), schema.field(i), safe=safe)
                for i in range(len(schema))
            ],
            schema=schema
        )


def safe_datatype(dtype: Union[DataType, Field, Any]) -> DataType:
    if isinstance(dtype, DataType):
        return dtype
    if isinstance(dtype, str):
        if dtype in STRING_DATATYPES:
            return STRING_DATATYPES[dtype]()
        elif '(' in dtype:
            key, args = dtype.split("(", 1)
            return STRING_DATATYPES[key](*(
                int(_) if _.strip().isnumeric() else _.strip() for _ in args[:-1].split(",")
            ))
    elif isinstance(dtype, Field):
        return dtype.type
    return ALL_DATATYPES[dtype]()
