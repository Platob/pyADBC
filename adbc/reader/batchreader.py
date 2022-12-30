from typing import Generator, Any, Iterable, Union, Optional

from arrow_odbc import BatchReader as _BatchReader
from pyarrow import RecordBatchReader, Schema, RecordBatch, Table, DataType,\
    schema as schema_builder, field as field_builder

__all__ = [
    "BatchReader"
]

from adbc.dtype import cast_batch, intersect_schemas, safe_datatype


class BatchReader:

    @classmethod
    def from_arrow(
        cls,
        batches: Union[RecordBatchReader, _BatchReader, RecordBatch, Table],
        chunk_size: Optional[int] = None
    ):
        if isinstance(batches, _BatchReader):
            return BatchReader(batches.schema, batches, persisted=False)
        elif isinstance(batches, RecordBatch):
            if chunk_size:
                return BatchReader(
                    batches.schema,
                    Table.from_batches([batches], batches.schema).to_batches(chunk_size),
                    persisted=True
                )
            return BatchReader(batches.schema, [batches], persisted=True)
        elif isinstance(batches, Table):
            return BatchReader(batches.schema, batches.to_batches(chunk_size), persisted=True)
        elif isinstance(batches, RecordBatchReader):
            return BatchReader(batches.schema, batches, persisted=False)
        else:
            raise TypeError("Cannot build BatchReader from object.__class__=%s" % batches.__class__)

    def __init__(
        self,
        schema: Schema,
        batches: Union[Iterable[RecordBatch], Any],
        persisted: bool = False
    ):
        self.schema = schema
        self.batches = batches
        self.persisted = persisted

    def __call__(self, *args, **kwargs):
        return self.batches

    def __iter__(self):
        return (_ for _ in self.batches)

    @property
    def batches(self) -> Iterable[RecordBatch]:
        # for custom implementations, must return a stream of RecordBatch
        return self._batches

    @batches.setter
    def batches(self, batches):
        self._batches = batches

    def persist(self):
        if self.persisted:
            return BatchReader(self.schema, list(self.batches))
        return self

    def close(self):
        pass

    def cast(self, schema: Schema, safe: bool = True, fill_empty: bool = True, drop: bool = False):
        _schema = schema if (fill_empty and not drop) else intersect_schemas(self.schema, schema, True)
        return BatchReader(
            _schema,
            (cast_batch(_, _schema, safe, fill_empty, drop) for _ in self.batches)
        )

    def cast_columns(self, columns: dict[str, Union[DataType, str]], safe: bool = True):
        columns = {
            k: safe_datatype(v)
            for k, v in columns.items()
        }
        return self.cast(
            schema_builder(
                [
                    field_builder(field.name, columns[field.name], field.nullable) if field.name in columns else field
                    for field in self.schema
                ]
            ),
            safe=safe, fill_empty=True, drop=False
        )

    def rows(self) -> Generator[tuple[Any], None, None]:
        for batch in self.batches:
            batch = batch.to_pydict()
            for row in zip(*(batch[k] for k in batch)):
                yield row

    def read_all(self):
        return Table.from_batches(self.batches, self.schema)
