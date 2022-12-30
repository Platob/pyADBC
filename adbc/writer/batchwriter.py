from abc import abstractmethod

from adbc.reader import BatchReader

__all__ = [
    "BatchWriter"
]


class BatchWriter:

    @abstractmethod
    def write_batches(
        self,
        batches: BatchReader,
        chunk_size: int = 65536,
        cast: bool = True,
        safe: bool = True,
        append: bool = True,
        **kwargs
    ):
        raise NotImplementedError("Not implemented write arrow batch for %s" % repr(self))
