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
- The databend-sqlalchemy use [databend-py](https://github.com/datafuselabs/databend-py) as internal driver when version < v0.4.0, but when version >= v0.4.0 it use [databend driver python binding](https://github.com/datafuselabs/bendsql/blob/main/bindings/python/README.md) as internal driver. The only difference between the two is that the connection parameters provided in the DSN are different. When using the corresponding version, you should refer to the connection parameters provided by the corresponding Driver.


