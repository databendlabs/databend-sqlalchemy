#!/usr/bin/env python

from sqlalchemy.testing import config, fixture, fixtures, util
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy import Table, Column, Integer, String, func, MetaData, schema, cast, literal_column

from databend_sqlalchemy import (
    CopyIntoTable, CopyIntoLocation, CopyIntoTableOptions, CSVFormat, ParquetFormat,
    GoogleCloudStorage, Compression, FileColumnClause
)

class CompileDatabendCopyIntoTableTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = "databend"

    def test_copy_into_table(self):
        m = MetaData()
        tbl = Table(
            'atable', m, Column("id", Integer),
            schema="test_schema",
        )

        copy_into = CopyIntoTable(
            target=tbl,
            from_=GoogleCloudStorage(
                uri='gcs://some-bucket/a/path/to/files',
                credentials='XYZ',
            ),
            #files='',
            #pattern='',
            file_format=CSVFormat(
                record_delimiter='\n',
                field_delimiter=',',
                quote='"',
                #escape='\\',
                #skip_header=1,
                #nan_display=''
                #null_display='',
                error_on_column_mismatch=False,
                #empty_field_as='STRING',
                output_header=True,
                #binary_format='',
                compression=Compression.GZIP
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
            )
        )


        self.assert_compile(
            copy_into,
            ("COPY INTO test_schema.atable"
            " FROM 'gcs://some-bucket/a/path/to/files' "
            "CONNECTION = ("
            "  ENDPOINT_URL = 'https://storage.googleapis.com' "
            "  CREDENTIAL = 'XYZ' "
            ")"
            " FILE_FORMAT = (TYPE = CSV, "
            "RECORD_DELIMITER = '\\n', FIELD_DELIMITER = ',', QUOTE = '\"', OUTPUT_HEADER = TRUE, COMPRESSION = GZIP) "
             )
        )

    def test_copy_into_table_sub_select_string_columns(self):
        m = MetaData()
        tbl = Table(
            'atable', m, Column("id", Integer),
            schema="test_schema",
        )

        copy_into = CopyIntoTable(
            target=tbl,
            from_=FileColumnClause(
                columns='$1, $2, $3',
                from_=GoogleCloudStorage(
                    uri='gcs://some-bucket/a/path/to/files',
                    credentials='XYZ',
                )
            ),
            file_format=CSVFormat(),
        )

        self.assert_compile(
            copy_into,
            ("COPY INTO test_schema.atable"
             " FROM (SELECT $1, $2, $3"
             " FROM 'gcs://some-bucket/a/path/to/files' "
             "CONNECTION = ("
             "  ENDPOINT_URL = 'https://storage.googleapis.com' "
             "  CREDENTIAL = 'XYZ' "
             ")"
             ") FILE_FORMAT = (TYPE = CSV)"
             )
        )

    def test_copy_into_table_sub_select_column_clauses(self):
        m = MetaData()
        tbl = Table(
            'atable', m, Column("id", Integer),
            schema="test_schema",
        )

        copy_into = CopyIntoTable(
            target=tbl,
            from_=FileColumnClause(
                columns=[func.IF(literal_column("$1") == 'xyz', 'NULL', 'NOTNULL')],
                # columns='$1, $2, $3',
                from_=GoogleCloudStorage(
                    uri='gcs://some-bucket/a/path/to/files',
                    credentials='XYZ',
                )
            ),
            file_format=CSVFormat(),
        )

        self.assert_compile(
            copy_into,
            ("COPY INTO test_schema.atable"
             " FROM (SELECT IF($1 = %(1_1)s, %(IF_1)s, %(IF_2)s)"
             " FROM 'gcs://some-bucket/a/path/to/files' "
             "CONNECTION = ("
             "  ENDPOINT_URL = 'https://storage.googleapis.com' "
             "  CREDENTIAL = 'XYZ' "
             ")"
             ") FILE_FORMAT = (TYPE = CSV)"
             ),
            checkparams={
                "1_1": "xyz",
                "IF_1": "NULL",
                "IF_2": "NOTNULL"
            },
        )
