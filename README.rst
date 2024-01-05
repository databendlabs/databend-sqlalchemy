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
        for i in cursor.next():
            print(i)




Compatibility
---------------

- If databend version >= v0.9.0 or later, you need to use databend-sqlalchemy version >= v0.1.0.
- The databend-sqlalchemy use [databend-py](https://github.com/datafuselabs/databend-py) as internal driver when version < v0.4.0, but when version >= v0.4.0 it use [databend driver python binding](https://github.com/datafuselabs/bendsql/blob/main/bindings/python/README.md) as internal driver. The only difference between the two is that the connection parameters provided in the DSN are different. When using the corresponding version, you should refer to the connection parameters provided by the corresponding Driver.
