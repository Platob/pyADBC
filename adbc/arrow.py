from typing import Union, Iterable, Optional, Tuple, Generator

from pyarrow import Table, RecordBatch
import pyarrow.compute as pc

__all__ = [
    "partitions",
    "download_tzdata_windows"
]


def partitions(
    table: Union[Table, RecordBatch],
    by: Optional[Iterable[str]] = None,
    partition_values: Optional[dict] = None
) -> Generator[Tuple[dict, Table], None, None]:
    if partition_values is None:
        partition_values = {}
    if by is None:
        yield partition_values, table
    elif isinstance(by, str):
        for value in pc.unique(table[by]):
            partition_values[by] = value.as_py()
            yield partition_values, table.filter(pc.equal(table[by], value))
    elif len(by) == 1:
        for _ in partitions(table, by[0], partition_values):
            yield _
    else:
        for value in pc.unique(table[by[0]]):
            partition_values[by[0]] = value.as_py()

            for partition in partitions(
                table.filter(pc.equal(table[by[0]], value)),
                by[1:],
                partition_values.copy()
            ):
                yield partition


def download_tzdata_windows(
    base_dir=None,
    year=2022,
    name="tzdata"
):
    import os
    import tarfile
    import urllib3

    http = urllib3.PoolManager()
    folder = base_dir if base_dir else os.path.join(os.path.expanduser('~'), "Downloads")
    tz_path = os.path.join(folder, "tzdata.tar.gz")

    with open(tz_path, "wb") as f:
        f.write(http.request('GET', f'https://data.iana.org/time-zones/releases/tzdata{year}f.tar.gz').data)

    folder = os.path.join(folder, name)

    if not os.path.exists(folder):
        os.makedirs(folder)

    tarfile.open(tz_path).extractall(folder)

    with open(os.path.join(folder, "windowsZones.xml"), "wb") as f:
        f.write(http.request('GET',
                             f'https://raw.githubusercontent.com/unicode-org/cldr/master/common/supplemental/windowsZones.xml').data)
