# pyADBC
Python based Arrow Data Base Connector

Stream pyarrow.RecordBatches

## MSSQL

### Read
```python
from adbc.mssql import *
odbc_uri = "Server=localhost\SQLEXPRESS01;Database=master;DRIVER={ODBC Driver 18 for SQL Server};"
batch_reader = MSSQL(odbc_uri).arrow_batches(
    "select top 5 * from table",
    batch_size=65536, # num rows for each batch
    max_text_size=256, # if there is varchar(max), set max size
    max_binary_size=256, # if there is binary(max), set max size
    lazy=False # True=make stream callable several times
)
for batch in batch_reader:
    print(batch)
```

## Athena

### Write
```python
from adbc.athena import Athena
s = Athena("eu-west-1", profile_name="default")
writer = s.write(
    "name",
    schema="database",
    catalog="AwsDataCatalog"
)

for path in writer.write_batches(
    batch_reader,
    append=True # False to delete dir contents
):
    print(path)
```