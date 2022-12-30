import unittest

from adbc.mssql import MSSQL


class MSSQLTest(unittest.TestCase):
    uri = "Server=localhost\SQLEXPRESS01;Database=master;DRIVER={ODBC Driver 18 for SQL " \
          "Server};Trusted_Connection=yes;TrustServerCertificate=YES;"
    server = MSSQL(uri=uri)

    def test_uri(self):
        self.assertEqual(self.server.uri, self.uri)

    def test_read(self):
        d = self.server.arrow_batches("select * from PYMSA_UNITTEST", 1000)
        for batch in d:
            print(batch)

    def test_read_rows(self):
        d = self.server.arrow_batches("select * from PYMSA_UNITTEST", 1000)
        for row in d.rows():
            print(row)

    def test_table_schema(self):
        schema = self.server.table_schema("PYMSA_UNITTEST")
        print(schema)

    def test_write(self):
        data = self.server.arrow_batches("select top 4 null as int, 'b' as string, datetime2 from PYMSA_UNITTEST", 1000)
        self.server.write("PYMSA_UNITTEST").write_batches(data, chunk_size=10, cast=True)
        print(self.server.arrow_batches("select * from PYMSA_UNITTEST", 10).read_all().to_pandas())


if __name__ == '__main__':
    unittest.main()
