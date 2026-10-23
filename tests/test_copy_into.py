#!/usr/bin/env python

from sqlalchemy.testing import config, fixture, fixtures, eq_
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    func,
    MetaData,
    schema,
    cast,
    literal_column,
    text,
)

from databend_sqlalchemy import (
    CopyIntoTable,
    CopyIntoLocation,
    CopyIntoTableOptions,
    CopyIntoLocationOptions,
    CSVFormat,
    ParquetFormat,
    GoogleCloudStorage,
    Compression,
    FileColumnClause,
    StageClause,
)


class CompileDatabendCopyIntoTableTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = "databend"

    def test_copy_into_table(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            schema="test_schema",
        )

        copy_into = CopyIntoTable(
            target=tbl,
            from_=GoogleCloudStorage(
                uri="gcs://some-bucket/a/path/to/files",
                credentials="XYZ",
            ),
            # files='',
            # pattern='',
            file_format=CSVFormat(
                record_delimiter="\n",
                field_delimiter=",",
                quote='"',
                # escape='\\',
                # skip_header=1,
                # nan_display=''
                # null_display='',
                error_on_column_mismatch=False,
                # empty_field_as='STRING',
                output_header=True,
                # binary_format='',
                compression=Compression.GZIP,
            ),
            options=CopyIntoTableOptions(
                size_limit=None,
                purge=None,
                force=None,
                disable_variant_check=None,
                on_error=None,
                max_files=None,
                return_failed_only=None,
                column_match_mode=None,
            ),
        )

        self.assert_compile(
            copy_into,
            (
                "COPY INTO test_schema.atable"
                " FROM 'gcs://some-bucket/a/path/to/files' "
                "CONNECTION = ("
                "  ENDPOINT_URL = 'https://storage.googleapis.com' "
                "  CREDENTIAL = 'XYZ' "
                ")"
                " FILE_FORMAT = (TYPE = CSV, "
                "RECORD_DELIMITER = '\\n', FIELD_DELIMITER = ',', QUOTE = '\"', OUTPUT_HEADER = TRUE, COMPRESSION = GZIP) "
            ),
        )

    def test_copy_into_table_sub_select_string_columns(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            schema="test_schema",
        )

        copy_into = CopyIntoTable(
            target=tbl,
            from_=FileColumnClause(
                columns="$1, $2, $3",
                from_=GoogleCloudStorage(
                    uri="gcs://some-bucket/a/path/to/files",
                    credentials="XYZ",
                ),
            ),
            file_format=CSVFormat(),
        )

        self.assert_compile(
            copy_into,
            (
                "COPY INTO test_schema.atable"
                " FROM (SELECT $1, $2, $3"
                " FROM 'gcs://some-bucket/a/path/to/files' "
                "CONNECTION = ("
                "  ENDPOINT_URL = 'https://storage.googleapis.com' "
                "  CREDENTIAL = 'XYZ' "
                ")"
                ") FILE_FORMAT = (TYPE = CSV)"
            ),
        )

    def test_copy_into_table_sub_select_column_clauses(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            schema="test_schema",
        )

        copy_into = CopyIntoTable(
            target=tbl,
            from_=FileColumnClause(
                columns=[func.IF(literal_column("$1") == "xyz", "NULL", "NOTNULL")],
                # columns='$1, $2, $3',
                from_=GoogleCloudStorage(
                    uri="gcs://some-bucket/a/path/to/files",
                    credentials="XYZ",
                ),
            ),
            file_format=CSVFormat(),
        )

        self.assert_compile(
            copy_into,
            (
                "COPY INTO test_schema.atable"
                " FROM (SELECT IF($1 = %(1_1)s, %(IF_1)s, %(IF_2)s)"
                " FROM 'gcs://some-bucket/a/path/to/files' "
                "CONNECTION = ("
                "  ENDPOINT_URL = 'https://storage.googleapis.com' "
                "  CREDENTIAL = 'XYZ' "
                ")"
                ") FILE_FORMAT = (TYPE = CSV)"
            ),
            checkparams={"1_1": "xyz", "IF_1": "NULL", "IF_2": "NOTNULL"},
        )

    def test_copy_into_table_files(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            schema="test_schema",
        )

        copy_into = CopyIntoTable(
            target=tbl,
            from_=GoogleCloudStorage(
                uri="gcs://some-bucket/a/path/to/files",
                credentials="XYZ",
            ),
            files=['one','two','three'],
            file_format=CSVFormat(),
        )

        self.assert_compile(
            copy_into,
            (
                "COPY INTO test_schema.atable"
                " FROM 'gcs://some-bucket/a/path/to/files' "
                "CONNECTION = ("
                "  ENDPOINT_URL = 'https://storage.googleapis.com' "
                "  CREDENTIAL = 'XYZ' "
                ") FILES = ('one', 'two', 'three')"
                " FILE_FORMAT = (TYPE = CSV)"
            ),
        )


class CopyIntoResultTest(fixtures.TablesTest):
    run_create_tables = "each"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "random_data",
            metadata,
            Column("id", Integer),
            Column("data", String(50)),
            databend_engine='Random',
        )
        Table(
            "loaded",
            metadata,
            Column("id", Integer),
            Column("data", String(50)),
        )

    def test_copy_into_stage_and_table(self, connection):
        # create stage
        connection.execute(text('CREATE OR REPLACE STAGE mystage'))
        # copy into stage from random table limiting 1000
        table = self.tables.random_data
        query = table.select().limit(1000)

        copy_into = CopyIntoLocation(
            target=StageClause(
                name='mystage'
            ),
            from_=query,
            file_format=ParquetFormat(),
            options=CopyIntoLocationOptions()
        )
        r = connection.execute(
            copy_into
        )
        eq_(r.rowcount, 1000)
        copy_into_results = r.context.copy_into_location_results()
        eq_(copy_into_results['rows_unloaded'], 1000)
        # eq_(copy_into_results['input_bytes'], 16250) # input bytes will differ, the table is random
        # eq_(copy_into_results['output_bytes'], 4701) # output bytes differs

        # now copy into table

        copy_into_table = CopyIntoTable(
            target=self.tables.loaded,
            from_=StageClause(
                name='mystage'
            ),
            file_format=ParquetFormat(),
            options=CopyIntoTableOptions()
        )
        r = connection.execute(
            copy_into_table
        )
        eq_(r.rowcount, 1000)
        copy_into_table_results = r.context.copy_into_table_results()
        assert len(copy_into_table_results) == 1
        result = copy_into_table_results[0]
        assert result['file'].endswith('.parquet')
        eq_(result['rows_loaded'], 1000)
        eq_(result['errors_seen'], 0)
        eq_(result['first_error'], None)
        eq_(result['first_error_line'], None)


