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
            f"databend://{username}:{password}@{host_port_name}/{database_name}?secure=false"
        )
        connection = engine.connect()
        result = connection.execute(text("SELECT 1"))
        assert len(result.fetchall()) == 1

        import connector
        cursor = connector.connect('http://root:@localhost:8081').cursor()
        cursor.execute('SELECT * FROM test')
        # print(cursor.fetchone())
        # print(cursor.fetchall())
        for i in cursor.next():
            print(i)




Compatibility
---------------

If databend version >= v0.9.0 or later, you need to use databend-sqlalchemy version >= v0.1.0.
