from typing import Callable, Optional, Generator

from pyarrow import Schema, RecordBatch

from adbc.dtype import cast_batch
from adbc.reader import BatchReader

__all__ = [
    "LazyReader"
]


class LazyReader(BatchReader):

    def __init__(
        self,
        method: Callable,
        schema: Optional[Schema] = None,
        cast: bool = False,
        *args,
        **kwargs: dict
    ):
        if schema is None:
            for _ in method(*args, **kwargs):
                schema = _.schema
                cast = False
                break
        super().__init__(schema, None)
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.safe_cast = cast

    def __call__(self, *args, **kwargs):
        return self.method(*self.args, **self.kwargs)

    @property
    def batches(self) -> Generator[RecordBatch, None, None]:
        return (
            cast_batch(_, self.schema) if self.safe_cast else _
            for _ in self()
        )

    @batches.setter
    def batches(self, batches):
        self._batches = batches
