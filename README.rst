databend-sqlalchemy
===================

Databend dialect for SQLAlchemy.

Installation
------------

The package is installable through PIP::

    pip install databend-sqlalchemy

Usage
-----

The DSN format is similar to that of regular Postgres::

        from sqlalchemy import create_engine, text
        from sqlalchemy.engine.base import Connection, Engine
        engine = create_engine(
            f"databend://{username}:{password}@{host_port_name}/{database_name}?sslmode=disable"
        )
        connection = engine.connect()
        result = connection.execute(text("SELECT 1"))
        assert len(result.fetchall()) == 1

        import connector
        cursor = connector.connect('databend://root:@localhost:8000?sslmode=disable').cursor()
        cursor.execute('SELECT * FROM test')
        # print(cursor.fetchone())
        # print(cursor.fetchall())
        for row in cursor:
            print(row)


Merge Command Support
---------------------

Databend SQLAlchemy supports upserts via its `Merge` custom expression.
See [Merge](https://docs.databend.com/sql/sql-commands/dml/dml-merge) for full documentation.

The Merge command can be used as below::

        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import MetaData, create_engine
        from databend_sqlalchemy.databend_dialect import Merge

        engine = create_engine(db.url, echo=False)
        session = sessionmaker(bind=engine)()
        connection = engine.connect()

        meta = MetaData()
        meta.reflect(bind=session.bind)
        t1 = meta.tables['t1']
        t2 = meta.tables['t2']

        merge = Merge(target=t1, source=t2, on=t1.c.t1key == t2.c.t2key)
        merge.when_matched_then_delete().where(t2.c.marked == 1)
        merge.when_matched_then_update().where(t2.c.isnewstatus == 1).values(val = t2.c.newval, status=t2.c.newstatus)
        merge.when_matched_then_update().values(val=t2.c.newval)
        merge.when_not_matched_then_insert().values(val=t2.c.newval, status=t2.c.newstatus)
        connection.execute(merge)


Copy Into Command Support
---------------------

Databend SQLAlchemy supports copy into operations through it's CopyIntoTable and CopyIntoLocation methods
See [CopyIntoLocation](https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-location) or [CopyIntoTable](https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table) for full documentation.

The CopyIntoTable command can be used as below::

        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import MetaData, create_engine
        from databend_sqlalchemy import (
            CopyIntoTable, GoogleCloudStorage, ParquetFormat, CopyIntoTableOptions,
            FileColumnClause, CSVFormat,
        )

        engine = create_engine(db.url, echo=False)
        session = sessionmaker(bind=engine)()
        connection = engine.connect()

        meta = MetaData()
        meta.reflect(bind=session.bind)
        t1 = meta.tables['t1']
        t2 = meta.tables['t2']
        gcs_private_key = 'full_gcs_json_private_key'
        case_sensitive_columns = True

        copy_into = CopyIntoTable(
            target=t1,
            from_=GoogleCloudStorage(
                uri='gcs://bucket-name/path/to/file',
                credentials=base64.b64encode(gcs_private_key.encode()).decode(),
            ),
            file_format=ParquetFormat(),
            options=CopyIntoTableOptions(
                force=True,
                column_match_mode='CASE_SENSITIVE' if case_sensitive_columns else None,
            )
        )
        result = connection.execute(copy_into)
        result.fetchall()  # always call fetchall() to ensure the cursor executes to completion

        # More involved example with column selection clause that can be altered to perform operations on the columns during import.

        copy_into = CopyIntoTable(
            target=t2,
            from_=FileColumnClause(
                columns=', '.join([
                    f'${index + 1}'
                    for index, column in enumerate(t2.columns)
                ]),
                from_=GoogleCloudStorage(
                    uri='gcs://bucket-name/path/to/file',
                    credentials=base64.b64encode(gcs_private_key.encode()).decode(),
                )
            ),
            pattern='*.*',
            file_format=CSVFormat(
                record_delimiter='\n',
                field_delimiter=',',
                quote='"',
                escape='',
                skip_header=1,
                empty_field_as='NULL',
                compression=Compression.AUTO,
            ),
            options=CopyIntoTableOptions(
                force=True,
            )
        )
        result = connection.execute(copy_into)
        result.fetchall()  # always call fetchall() to ensure the cursor executes to completion

The CopyIntoLocation command can be used as below::

        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import MetaData, create_engine
        from databend_sqlalchemy import (
            CopyIntoLocation, GoogleCloudStorage, ParquetFormat, CopyIntoLocationOptions,
        )

        engine = create_engine(db.url, echo=False)
        session = sessionmaker(bind=engine)()
        connection = engine.connect()

        meta = MetaData()
        meta.reflect(bind=session.bind)
        t1 = meta.tables['t1']
        gcs_private_key = 'full_gcs_json_private_key'

        copy_into = CopyIntoLocation(
            target=GoogleCloudStorage(
                uri='gcs://bucket-name/path/to/target_file',
                credentials=base64.b64encode(gcs_private_key.encode()).decode(),
            ),
            from_=select(t1).where(t1.c['col1'] == 1),
            file_format=ParquetFormat(),
            options=CopyIntoLocationOptions(
                single=True,
                overwrite=True,
                include_query_id=False,
                use_raw_path=True,
            )
        )
        result = connection.execute(copy_into)
        result.fetchall()  # always call fetchall() to ensure the cursor executes to completion

Table Options
---------------------

Databend SQLAlchemy supports databend specific table options for Engine, Cluster Keys and Transient tables

The table options can be used as below::

        from sqlalchemy import Table, Column
        from sqlalchemy import MetaData, create_engine

        engine = create_engine(db.url, echo=False)

        meta = MetaData()
        # Example of Transient Table
        t_transient = Table(
            "t_transient",
            meta,
            Column("c1", Integer),
            databend_transient=True,
        )

        # Example of Engine
        t_engine = Table(
            "t_engine",
            meta,
            Column("c1", Integer),
            databend_engine='Memory',
        )

        # Examples of Table with Cluster Keys
        t_cluster_1 = Table(
            "t_cluster_1",
            meta,
            Column("c1", Integer),
            databend_cluster_by=[c1],
        )
        #
        c = Column("id", Integer)
        c2 = Column("Name", String)
        t_cluster_2 = Table(
            't_cluster_2',
            meta,
            c,
            c2,
            databend_cluster_by=[cast(c, String), c2],
        )

        meta.create_all(engine)



Compatibility
---------------

- If databend version >= v0.9.0 or later, you need to use databend-sqlalchemy version >= v0.1.0.
- The databend-sqlalchemy use [databend-py](https://github.com/databendlabs/databend-py) as internal driver when version < v0.4.0, but when version >= v0.4.0 it use [databend driver python binding](https://github.com/databendlabs/bendsql/blob/main/bindings/python/README.md) as internal driver. The only difference between the two is that the connection parameters provided in the DSN are different. When using the corresponding version, you should refer to the connection parameters provided by the corresponding Driver.
