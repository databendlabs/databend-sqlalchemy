#!/usr/bin/env python
#
# See http://www.python.org/dev/peps/pep-0249/
#
# Many docstrings in this file are based on the PEP, which is in the public domain.

from __future__ import absolute_import
from __future__ import unicode_literals
import re
import uuid
from databend_py import Client
from datetime import datetime
from errors import ServerException, NotSupportedError

# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'  # Python extended format codes, e.g. ...WHERE name=%(name)s


class Error(Exception):
    """Exception that is the base class of all other error exceptions.
    You can use this to catch all errors with one single except statement.
    """
    pass


class ParamEscaper(object):
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
            item = item.decode('utf-8')
        return "'{}'".format(item.replace("\\", "\\\\").replace("'", "\\'").replace("$", "$$"))

    def escape_item(self, item):
        if item is None:
            return 'NULL'
        elif isinstance(item, (int, float)):
            return self.escape_number(item)
        elif isinstance(item, datetime):
            return self.escape_string(item.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            raise Exception("Unsupported object {}".format(item))


_escaper = ParamEscaper()


# Patch ORM library
@classmethod
def create_ad_hoc_field(cls, db_type):
    # Enums
    if db_type.startswith('Enum'):
        db_type = 'String'  # enum.Eum is not comparable
    # Arrays
    if db_type.startswith('Array'):
        return 'Array'
    # FixedString
    if db_type.startswith('FixedString'):
        db_type = 'String'

    if db_type == 'LowCardinality(String)':
        db_type = 'String'

    if db_type.startswith('DateTime'):
        db_type = 'DateTime'

    if db_type.startswith('Nullable'):
        return 'Nullable'


#
# Connector interface
#

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


class Connection(Client):
    """
        These objects are small stateless factories for cursors, which do all the real work.
    """

    def __init__(self, db_url='http://root:@localhost:8081'):
        super(Connection, self).__init__(db_url)
        self.db_url = db_url
        self.client = Client.from_url(db_url)

    def close(self):
        pass

    def commit(self):
        pass

    def cursor(self):
        return Cursor(self)

    def rollback(self):
        raise NotSupportedError("Transactions are not supported")  # pragma: no cover


class Cursor(object):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """
    _STATE_NONE = None
    _STATE_RUNNING = "Running"
    _STATE_SUCCEEDED = "Succeeded"

    def __init__(self, connector):
        self._db = connector.client
        self._reset_state()

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        self._uuid = None
        self._columns = None
        self._rownumber = 0
        # Internal helper state
        self._state = self._STATE_NONE
        self._data = None
        self._columns = None

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
        # Sleep until we're done or we got the columns
        if self._columns is None:
            return []
        return [
            col for col in self._columns
        ]

    def close(self):
        pass

    def execute(self, operation, parameters=None, is_response=True):
        """Prepare and execute a database operation (query or command). """
        if parameters is None or not parameters:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        self._reset_state()

        self._state = self._STATE_RUNNING
        self._uuid = uuid.uuid1()

        if is_response:
            response = self._db.execute(sql,with_column_types=True)
            self._process_response(response)

    def executemany(self, operation, seq_of_parameters):
        """Prepare a database operation (query or command) and then execute it against all parameter
        sequences or mappings found in the sequence ``seq_of_parameters``.

        Only the final result set is retained.

        Return values are not defined.
        """
        values_list = []
        RE_INSERT_VALUES = re.compile(
            r"\s*((?:INSERT|REPLACE)\s.+\sVALUES?\s*)" +
            r"(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))" +
            r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",
            re.IGNORECASE | re.DOTALL)

        m = RE_INSERT_VALUES.match(operation)
        if m:
            q_prefix = m.group(1) % ()
            q_values = m.group(2).rstrip()

            for parameters in seq_of_parameters[:-1]:
                values_list.append(q_values % _escaper.escape_args(parameters))
            query = '{} {};'.format(q_prefix, ','.join(values_list))
            return self._db.raw(query)
        for parameters in seq_of_parameters[:-1]:
            self.execute(operation, parameters, is_response=False)

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or ``None`` when
        no more data is available. """
        if self._state == self._STATE_NONE:
            raise Exception("No query yet")
        if not self._data:
            return None
        else:
            self._rownumber += 1
            return self._data.pop(0)

    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a
        list of tuples). An empty sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If it is not given, the
        cursor's arraysize determines the number of rows to be fetched. The method should try to
        fetch as many rows as indicated by the size parameter. If this is not possible due to the
        specified number of rows not being available, fewer rows may be returned.
        """
        if self._state == self._STATE_NONE:
            raise Exception("No query yet")

        if size is None:
            size = 1

        if not self._data:
            return []
        else:
            if len(self._data) > size:
                result, self._data = self._data[:size], self._data[size:]
            else:
                result, self._data = self._data, []
            self._rownumber += len(result)
            return result

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a list of tuples).
        """
        if self._state == self._STATE_NONE:
            raise Exception("No query yet")

        if not self._data:
            return []
        else:
            result, self._data = self._data, []
            self._rownumber += len(result)
            return result

    def __next__(self):
        """Return the next row from the currently executing SQL statement using the same semantics
        as :py:meth:`fetchone`. A ``StopIteration`` exception is raised when the result set is
        exhausted.
        """
        one = self.fetchone()
        if one is None:
            raise StopIteration
        else:
            return one

    next = __next__

    def __iter__(self):
        """Return self to make cursors compatible to the iteration protocol."""
        return self

    def cancel(self):
        if self._state == self._STATE_NONE:
            raise ServerException("No query yet")
        if self._uuid is None:
            assert self._state == self._STATE_SUCCEEDED, "Query should be finished"
            return
        # Replace current running query to cancel it
        self._db.execute("SELECT 1")
        self._state = self._STATE_SUCCEEDED
        self._uuid = None
        self._data = None

    def poll(self):
        pass

    def _process_response(self, response):
        """ Update the internal state with the data from the response """
        assert self._state == self._STATE_RUNNING, "Should be running if processing response"
        cols = None
        data = []
        # [('column name','column type'),(data,data...)] example:
        # [('Field', 'String'), ('Type', 'String'), ('Null', 'String'), ('Default', 'String'), ('Extra', 'String'), ('x', 'INT', 'NO', '0', ''), ('y', 'VARCHAR', 'NO', '', '')]
        if not cols and len(response) != 0:
            cols = response[:len(response[-1])]
            data = response[len(response[-1]):]

        self._data = data
        self._columns = cols
        self._state = self._STATE_SUCCEEDED
