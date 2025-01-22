#!/usr/bin/env python
#
# See http://www.python.org/dev/peps/pep-0249/
#
# Many docstrings in this file are based on the PEP, which is in the public domain.
import decimal
import re
from datetime import datetime, date, time, timedelta
from databend_sqlalchemy.errors import Error, NotSupportedError

from databend_driver import BlockingDatabendClient

# PEP 249 module globals
apilevel = "2.0"
threadsafety = 2  # Threads may share the module and connections.
paramstyle = "pyformat"  # Python extended format codes, e.g. ...WHERE name=%(name)s
Binary = bytes  # to satisfy dialect.dbapi.Binary


class ParamEscaper:
    def escape_args(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return tuple(self.escape_item(x) for x in parameters)
        else:
            raise Exception("Unsupported param format: {}".format(parameters))

    def escape_number(self, item):
        return item

    def escape_string(self, item):
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode("utf-8")
        return "'{}'".format(
            item.replace("\\", "\\\\").replace("'", "\\'").replace("%", "%%")
        )

    def escape_item(self, item):
        if item is None:
            return "NULL"
        elif isinstance(item, (int, float)):
            return self.escape_number(item)
        elif isinstance(item, decimal.Decimal):
            return self.escape_number(item)
        elif isinstance(item, timedelta):
            return self.escape_string(f"{item.total_seconds()} seconds") + "::interval"
        elif isinstance(item, (datetime, date, time, timedelta)):
            return self.escape_string(item.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            return self.escape_string(item)


_escaper = ParamEscaper()


# Patch ORM library
@classmethod
def create_ad_hoc_field(cls, db_type):
    # Enums
    if db_type.startswith("Enum"):
        db_type = "String"  # enum.Eum is not comparable
    # Arrays
    if db_type.startswith("Array"):
        return "Array"
    # FixedString
    if db_type.startswith("FixedString"):
        db_type = "String"

    if db_type == "LowCardinality(String)":
        db_type = "String"

    if db_type.startswith("DateTime"):
        db_type = "DateTime"

    if db_type.startswith("Nullable"):
        return "Nullable"


#
# Connector interface
#


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


class Connection:
    """
    These objects are small stateless factories for cursors, which do all the real work.
    """

    def __init__(self, dsn="databend://root:@localhost:8000/?sslmode=disable"):
        self.client = BlockingDatabendClient(dsn)

    def close(self):
        pass

    def commit(self):
        pass

    def cursor(self):
        return Cursor(self.client.cursor())

    def rollback(self):
        raise NotSupportedError("Transactions are not supported")  # pragma: no cover


class Cursor:
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """

    def __init__(self, conn):
        self.inner = conn

    @property
    def rowcount(self):
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        try:
            return self.inner.description
        except Exception as e:
            raise Error(str(e)) from e

    def close(self):
        try:
            self.inner.close()
        except Exception as e:
            raise Error(str(e)) from e

    def mogrify(self, query, parameters):
        if parameters:
            query = query % _escaper.escape_args(parameters)
        return query

    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command)."""

        # ToDo - Fix this, which is preventing the execution of blank DDL sunch as CREATE INDEX statements which aren't currently supported
        # Seems hard to fix when statements are coming from metadata.create_all()
        if not operation:
            return

        try:
            query = self.mogrify(operation, parameters)
            query = query.replace("%%", "%")
            return self.inner.execute(query)
        except Exception as e:
            raise Error(str(e)) from e

    def executemany(self, operation, seq_of_parameters):
        """Prepare a database operation (query or command) and then execute it against all parameter
        sequences or mappings found in the sequence ``seq_of_parameters``.

        Only the final result set is retained.

        Return values are not defined.
        """
        values_list = []
        RE_INSERT_VALUES = re.compile(
            r"\s*((?:INSERT|REPLACE)\s.+\sVALUES?\s*)"
            + r"(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))"
            + r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",
            re.IGNORECASE | re.DOTALL,
        )

        m = RE_INSERT_VALUES.match(operation)
        if m:
            try:
                q_prefix = m.group(1)
                q_values = m.group(2).rstrip()

                for parameters in seq_of_parameters:
                    values_list.append(q_values % _escaper.escape_args(parameters))
                query = "{} {};".format(q_prefix, ",".join(values_list))
                return self.inner.execute(query)
            except Exception as e:
                # We have to raise dbAPI error
                raise Error(str(e)) from e
        else:
            for parameters in seq_of_parameters:
                self.execute(operation, parameters)

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or ``None`` when
        no more data is available."""
        try:
            row = self.inner.fetchone()
            if row is None:
                return None
            return row.values()
        except Exception as e:
            raise Error(str(e)) from e

    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a
        list of tuples). An empty sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If it is not given, the
        cursor's arraysize determines the number of rows to be fetched. The method should try to
        fetch as many rows as indicated by the size parameter. If this is not possible due to the
        specified number of rows not being available, fewer rows may be returned.
        """
        try:
            rows = self.inner.fetchmany(size)
            return [row.values() for row in rows]
        except Exception as e:
            raise Error(str(e)) from e

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a list of tuples).
        """
        try:
            rows = self.inner.fetchall()
            return [row.values() for row in rows]
        except Exception as e:
            raise Error(str(e)) from e

    def __next__(self):
        """Return the next row from the currently executing SQL statement using the same semantics
        as :py:meth:`fetchone`. A ``StopIteration`` exception is raised when the result set is
        exhausted.
        """
        try:
            return self.inner.__next__()
        except StopIteration as e:
            raise e
        except Exception as e:
            raise Error(str(e)) from e

    next = __next__

    def __iter__(self):
        """Return self to make cursors compatible to the iteration protocol."""
        return self
