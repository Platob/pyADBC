import os
from typing import Callable, Optional, Any, Generator
from urllib.parse import quote_plus

from pyarrow import Schema
from pyarrow.fs import FileInfo, FileSelector, FileSystem, FileType, LocalFileSystem

from adbc.arrow import partitions
from adbc.enums import Protocol, FileFormat
from adbc.exception import TableNotFound
from adbc.reader import BatchReader
from adbc.server import Server, Connection
from adbc.writer.batchwriter import BatchWriter

__all__ = [
    "DataFileSystem", "DFSWriter"
]


class DataFileSystem(Server):

    def connect(self) -> Connection:
        return super(DataFileSystem, self).connect()

    @staticmethod
    def iter_dir_files(
        fs: FileSystem,
        path: str,
        allow_not_found: bool = False,
    ) -> Generator[FileInfo, None, None]:
        for ofs in fs.get_file_info(
            FileSelector(path, allow_not_found=allow_not_found, recursive=False)
        ):
            # <FileInfo for 'path': type=FileType.Directory>
            # or <FileInfo for 'path': type=FileType.File, size=0>
            if ofs.is_file:
                yield ofs
            elif ofs.type == FileType.Directory:
                for file in DataFileSystem.iter_dir_files(fs, ofs.path, allow_not_found):
                    yield file

    @staticmethod
    def get_local():
        return LocalFileSystem()

    @staticmethod
    def get_s3(region_name: str, profile_name: Optional[str] = None, **kwargs) -> "S3FileSystem":
        from pyarrow.fs import S3FileSystem
        if profile_name:
            from boto3 import Session
            credentials = Session(profile_name=profile_name).get_credentials()
            kwargs["secret_key"] = credentials.secret_key
            kwargs["access_key"] = credentials.access_key
            kwargs["session_token"] = credentials.token
        return S3FileSystem(region=region_name, **kwargs)

    def __init__(
        self,
        fs_builder: Callable[[Any], FileSystem],
        path_sep: str = "/",
        **fs_options
    ):
        super(DataFileSystem, self).__init__(protocol=Protocol.dfs)
        self.fs_builder = fs_builder
        self.fs_options = fs_options
        self.path_sep = path_sep

    def fs(self):
        return self.fs_builder(**self.fs_options)

    def table_schema(self, name: str, schema: Optional[str] = None, catalog: Optional[str] = None) -> Schema:
        raise TableNotFound("%s: Table '%s'" % (repr(self), name))

    def arrow_batches(
        self,
        query: str,
        batch_size: int = 65536,
        **kwargs
    ) -> BatchReader:
        raise NotImplementedError()

    def write(
        self,
        table: str,
        base_dir: str = "",
        file_format: str = FileFormat.parquet,
        file_writer: Optional[Callable] = None,
        partition_by: list[str] = (),
        schema: Optional[str] = None,
        catalog: Optional[str] = None,
        schema_arrow: Optional[Schema] = None
    ):
        return DFSWriter(
            self, table, base_dir, file_format, file_writer, partition_by,
            schema=schema, catalog=catalog, schema_arrow=schema_arrow
        )

# ---------------------------------------------- WRITER ----------------------------------------------


def parquet_file_writer(
    fs: FileSystem,
    folder: str,
    filename: str,
    schema: Schema,
    append: bool = True,
    path_sep: str = "/",
    **kwargs
):
    from pyarrow.parquet import ParquetWriter

    if not append:
        fs.delete_dir_contents(folder, missing_dir_ok=True)
    fs.create_dir(folder)

    return ParquetWriter(
        folder + path_sep + filename,
        schema,
        filesystem=fs,
        **kwargs
    )


class DFSWriter(BatchWriter):

    file_writers = {
        FileFormat.parquet: parquet_file_writer
    }

    def __init__(
        self,
        server: "DataFileSystem",
        table: str,
        base_dir: str,
        file_format: str,
        file_writer: Optional[Callable] = None,
        partition_by: list[str] = (),
        schema: Optional[str] = None,
        catalog: Optional[str] = None,
        schema_arrow: Optional[Schema] = None
    ):
        self.server = server
        self.table = table
        self.base_dir = base_dir
        self.file_format = file_format
        self.file_writer = file_writer if callable(file_writer) else self.file_writers[self.file_format]

        self.partition_by = partition_by
        self.schema = schema
        self.catalog = catalog
        self.schema_arrow = schema_arrow

    @property
    def path_sep(self):
        return self.server.path_sep

    def folder(self, partition_values: Optional[dict] = None):
        if partition_values:
            return self.base_dir + self.path_sep + self.path_sep.join((
                "%s=%s" % (k, quote_plus(v.decode() if isinstance(v, bytes) else str(v)))
                for k, v in partition_values.items()
            ))
        else:
            return self.base_dir

    def filename(self, seed: int = 16):
        return os.urandom(seed).hex() + "." + self.file_format

    def writer_builder(
        self,
        schema: Schema,
        partition_values: Optional[dict] = None,
        append: bool = True,
        **kwargs
    ):
        return self.file_writer(
            fs=self.server.fs(),
            folder=self.folder(partition_values),
            filename=self.filename(),
            schema=schema,
            append=append,
            path_sep=self.path_sep,
            **kwargs
        )

    def write_batches(
        self,
        batches: BatchReader,
        chunk_size: int = 65536,
        cast: bool = True,
        safe: bool = True,
        append: bool = True,
        max_file_rows: int = 4 * 1024 * 1024,
        **kwargs
    ) -> Generator[str, None, None]:
        if self.schema_arrow is None:
            try:
                table_schema = self.server.table_schema(self.table, self.schema, self.catalog)
            except TableNotFound:
                table_schema, cast = batches.schema, False
        else:
            table_schema = self.schema_arrow

        if cast:
            batches = batches.cast(table_schema, safe, True, False)

        if self.partition_by:
            writers: dict[tuple[Any], tuple[int, Any]] = dict()

            try:
                for batch in batches:
                    for pvalues, pbatch in partitions(batch, self.partition_by):
                        phash = tuple(pvalues.items())
                        if phash not in writers:
                            nrows, writer = writers[phash] = (
                                0,
                                self.writer_builder(table_schema, pvalues, append, **kwargs)
                            )
                        else:
                            nrows, writer = writers[phash]

                        while nrows + pbatch.num_rows >= max_file_rows:
                            # write the first dif
                            writer.write(pbatch.slice(0, max_file_rows - nrows), chunk_size)
                            writer.close()
                            yield writer.where
                            pbatch = pbatch.slice(max_file_rows - nrows, None)
                            nrows = 0
                            writer = self.writer_builder(table_schema, pvalues, True, **kwargs)

                        if pbatch.num_rows > 0:
                            writer.write(pbatch, chunk_size)
                            nrows += pbatch.num_rows
                        writers[phash] = (nrows, writer)
            except BaseException as e:
                if writers:
                    fs = self.server.fs()
                    try:
                        for k, v in writers.items():
                            _, writer = v
                            writer.close()
                            fs.delete_file(writer.where)
                        writers = dict()
                    except BaseException:
                        pass
                raise e
            finally:
                if writers:
                    fs = self.server.fs()
                    for k, v in writers.items():
                        nrows, writer = v
                        writer.close()
                        if nrows == 0:
                            fs.delete_file(writer.where)
                        else:
                            yield writer.where
        else:
            nrows = 0
            writer = self.writer_builder(table_schema, append=append, **kwargs)

            try:
                for batch in batches:
                    while nrows + batch.num_rows >= max_file_rows:
                        # write the first dif
                        writer.write(batch.slice(0, max_file_rows - nrows), chunk_size)
                        writer.close()
                        yield writer.where
                        batch = batch.slice(max_file_rows - nrows, None)
                        nrows = 0
                        writer = self.writer_builder(table_schema, None, True, **kwargs)

                    if batch.num_rows > 0:
                        writer.write(batch, chunk_size)
                        nrows += batch.num_rows
                    writer.write(batch, chunk_size)
            except BaseException as e:
                try:
                    writer.close()
                    self.server.fs().delete_file(writer.where)
                    writer = None
                except BaseException:
                    pass
                raise e
            finally:
                if writer is not None:
                    writer.close()
                    if nrows == 0:
                        self.server.fs().delete_file(writer.where)
                    else:
                        yield writer.where
