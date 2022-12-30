import os.path
from typing import Callable, Any

from pyarrow.filesystem import FileSystem

from adbc.filesystem import DataFileSystem


__all__ = [
    "LocalDataFileSystem"
]


class LocalDataFileSystem(DataFileSystem):

    def __init__(self, fs_builder: Callable[[Any], FileSystem] = DataFileSystem.get_local):
        super().__init__(fs_builder, os.path.sep)
