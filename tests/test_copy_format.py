#!/usr/bin/env python

from sqlalchemy.testing import fixtures, assert_raises_message, eq_
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy import Table, Column, String, text

from databend_sqlalchemy import (
    CopyIntoLocation,
    CopyIntoLocationOptions,
    CSVFormat,
    TSVFormat,
    NDJSONFormat,
    ParquetFormat,
    Compression,
    StageClause,
)
import sqlalchemy
from packaging import version


class CompileCopyFormatCompressionTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = "databend"

    _FORMATS = [
        (CSVFormat, "CSV"),
        (TSVFormat, "TSV"),
        (NDJSONFormat, "NDJSON"),
        (ParquetFormat, "PARQUET"),
    ]

    def test_omitted_compression_emits_no_option(self):
        for fmt_cls, type_ in self._FORMATS:
            self.assert_compile(fmt_cls(), f"FILE_FORMAT = (TYPE = {type_})")

    def test_explicit_none_is_emitted(self):
        # Omitting COMPRESSION means the server default, which for Parquet is ZSTD.
        for fmt_cls, type_ in self._FORMATS:
            self.assert_compile(
                fmt_cls(compression=Compression.NONE),
                f"FILE_FORMAT = (TYPE = {type_}, COMPRESSION = NONE)",
            )

    def test_explicit_codec_is_emitted(self):
        for fmt_cls, type_, codec in [
            (CSVFormat, "CSV", Compression.GZIP),
            (TSVFormat, "TSV", Compression.GZIP),
            (NDJSONFormat, "NDJSON", Compression.GZIP),
            (ParquetFormat, "PARQUET", Compression.ZSTD),
            (ParquetFormat, "PARQUET", Compression.SNAPPY),
        ]:
            self.assert_compile(
                fmt_cls(compression=codec),
                f"FILE_FORMAT = (TYPE = {type_}, COMPRESSION = {codec.value})",
            )

    def test_parquet_rejects_unsupported_codec(self):
        for codec in [Compression.GZIP, Compression.BZ2, Compression.ZIP]:
            assert_raises_message(
                TypeError,
                "Compression should be NONE, ZSTD, or SNAPPY.",
                ParquetFormat,
                compression=codec,
            )


class ParquetUnloadCompressionTest(fixtures.TablesTest):
    run_create_tables = "each"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table("repetitive", metadata, Column("data", String(200)))

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.repetitive.insert(),
            [{"data": "x" * 200} for _ in range(1000)],
        )

    def _unload(self, connection, compression):
        # Stages are account-wide, so each test needs its own under xdist.
        stage = f"parquet_codec_{compression.value if compression else 'default'}"
        connection.execute(text(f"CREATE OR REPLACE STAGE {stage}"))
        connection.execute(
            CopyIntoLocation(
                target=StageClause(name=stage),
                from_=self.tables.repetitive.select(),
                file_format=ParquetFormat(compression=compression),
                options=CopyIntoLocationOptions(single=True),
            )
        )
        name = connection.execute(text(f"LIST @{stage}")).scalar()
        sizes = connection.execute(
            text(
                "SELECT max_row_groups_size_compressed, "
                "max_row_groups_size_uncompressed "
                f"FROM inspect_parquet('@{stage}/{name}')"
            )
        ).one()
        connection.execute(text(f"DROP STAGE {stage}"))
        return sizes

    if version.parse(sqlalchemy.__version__) >= version.parse("2.0.0"):

        def test_explicit_none_writes_uncompressed(self, connection):
            compressed, uncompressed = self._unload(connection, Compression.NONE)
            eq_(compressed, uncompressed)

        def test_omitted_compression_uses_server_default(self, connection):
            compressed, uncompressed = self._unload(connection, None)
            assert compressed < uncompressed
