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

        import connector
        cursor = connector.connect('http://root:@localhost:8081').cursor()
        cursor.execute('SELECT * FROM test')
        # print(cursor.fetchone())
        # print(cursor.fetchall())
        for i in cursor.next():
            print(i)



