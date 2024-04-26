
from sqlalchemy.testing import config, fixture, fixtures, util
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy import Table, Column, Integer, String, func, MetaData, schema, cast


class CompileDatabendTableOptionsTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = "databend"

    def test_create_table_transient_on(self):
        m = MetaData()
        tbl = Table(
            'atable', m, Column("id", Integer),
            databend_transient=True,
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TRANSIENT TABLE atable (id INTEGER)")

    def test_create_table_transient_off(self):
        m = MetaData()
        tbl = Table(
            'atable', m, Column("id", Integer),
            databend_transient=False,
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER)")

    def test_create_table_engine(self):
        m = MetaData()
        tbl = Table(
            'atable', m, Column("id", Integer),
            databend_engine='Memory',
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) ENGINE=Memory")

    def test_create_table_cluster_by_column_str(self):
        m = MetaData()
        tbl = Table(
            'atable', m, Column("id", Integer),
            databend_cluster_by='id',
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) CLUSTER BY ( id )")

    def test_create_table_cluster_by_column_strs(self):
        m = MetaData()
        tbl = Table(
            'atable', m, Column("id", Integer), Column("Name", String),
            databend_cluster_by=['id', 'Name'],
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER, \"Name\" VARCHAR) CLUSTER BY ( id, \"Name\" )")

    def test_create_table_cluster_by_column_object(self):
        m = MetaData()
        c = Column("id", Integer)
        tbl = Table(
            'atable', m, c,
            databend_cluster_by=[c],
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) CLUSTER BY ( id )")

    def test_create_table_cluster_by_column_objects(self):
        m = MetaData()
        c = Column("id", Integer)
        c2 = Column("Name", String)
        tbl = Table(
            'atable', m, c, c2,
            databend_cluster_by=[c, c2],
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER, \"Name\" VARCHAR) CLUSTER BY ( id, \"Name\" )")

    def test_create_table_cluster_by_column_expr(self):
        m = MetaData()
        c = Column("id", Integer)
        c2 = Column("Name", String)
        tbl = Table(
            'atable', m, c, c2,
            databend_cluster_by=[cast(c, String), c2],
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER, \"Name\" VARCHAR) CLUSTER BY ( CAST(id AS VARCHAR), \"Name\" )")

    def test_create_table_cluster_by_str(self):
        m = MetaData()
        c = Column("id", Integer)
        c2 = Column("Name", String)
        tbl = Table(
            'atable', m, c, c2,
            databend_cluster_by="CAST(id AS VARCHAR), \"Name\"",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER, \"Name\" VARCHAR) CLUSTER BY ( CAST(id AS VARCHAR), \"Name\" )")

    #ToDo
    # def test_create_table_with_options(self):
    #     m = MetaData()
    #     tbl = Table(
    #         'atable', m, Column("id", Integer),
    #         databend_engine_options=(
    #             ("compression", "snappy"),
    #             ("storage_format", "parquet"),
    #         ))
    #     self.assert_compile(
    #         schema.CreateTable(tbl),
    #         "CREATE TABLE atable (id INTEGER)COMPRESSION=\"snappy\" STORAGE_FORMAT=\"parquet\"")


class ReflectDatabendTableOptionsTest(fixtures.TablesTest):
    __backend__ = True
    __only_on__ = "databend"

    # 'once', 'each', None
    run_inserts = "None"

    # 'each', None
    run_deletes = "None"

    @classmethod
    def define_tables(cls, metadata):
        Table(
        "t2_engine",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("Name", String),
            databend_engine="Memory",
        )
        c2 = Column("id", Integer, primary_key=True)
        Table(
            "t2_cluster_by_column",
            metadata,
            c2,
            Column("Name", String),
            databend_cluster_by=[c2, "Name"],
        )
        c3 = Column("id", Integer, primary_key=True)
        Table(
            "t3_cluster_by_expr",
            metadata,
            c3,
            Column("Name", String),
            databend_cluster_by=[cast(c3, String), "Name"],
        )
        c4 = Column("id", Integer, primary_key=True)
        Table(
            "t4_cluster_by_str",
            metadata,
            c4,
            Column("Name", String),
            databend_cluster_by='CAST(id AS STRING), "Name"',
        )

    def test_reflect_table_engine(self):
        m2 = MetaData()
        t1_ref = Table(
            "t2_engine", m2, autoload_with=config.db
        )
        assert t1_ref.dialect_options['databend']['engine'] == 'MEMORY'

    def test_reflect_table_cluster_by_column(self):
        m2 = MetaData()
        t2_ref = Table(
            "t2_cluster_by_column", m2, autoload_with=config.db
        )
        assert t2_ref.dialect_options['databend']['cluster_by'] == 'id, "Name"'

    def test_reflect_table_cluster_by_expr(self):
        m2 = MetaData()
        t3_ref = Table(
            "t3_cluster_by_expr", m2, autoload_with=config.db
        )
        assert t3_ref.dialect_options['databend']['cluster_by'] == 'CAST(id AS STRING), "Name"'

    def test_reflect_table_cluster_by_str(self):
        m2 = MetaData()
        t4_ref = Table(
            "t4_cluster_by_str", m2, autoload_with=config.db
        )
        assert t4_ref.dialect_options['databend']['cluster_by'] == 'CAST(id AS STRING), "Name"'