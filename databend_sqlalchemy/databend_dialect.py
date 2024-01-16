#!/usr/bin/env python
#
# Note: parts of the file come from https://github.com/snowflakedb/snowflake-sqlalchemy
#       licensed under the same Apache 2.0 License

import re
import sqlalchemy.types
import sqlalchemy.util
import sqlalchemy.types as sqltypes
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple, Union
from sqlalchemy import exc as sa_exc
from sqlalchemy import util as sa_util
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import compiler, expression, text
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.dialects.postgresql.base import PGCompiler, PGIdentifierPreparer
from sqlalchemy.types import (
    CHAR,
    DATE,
    DATETIME,
    INTEGER,
    SMALLINT,
    BIGINT,
    DECIMAL,
    TIME,
    TIMESTAMP,
    VARCHAR,
    BINARY,
    BOOLEAN,
    FLOAT,
    REAL,
    JSON,
)
from sqlalchemy.engine import ExecutionContext, default

# Column spec
colspecs = {}


# Type decorators
class ARRAY(sqltypes.TypeEngine):
    __visit_name__ = "ARRAY"


class MAP(sqltypes.TypeEngine):
    __visit_name__ = "MAP"

    def __init__(self, key_type, value_type):
        self.key_type = key_type
        self.value_type = value_type
        super(MAP, self).__init__()


# Type converters
ischema_names = {
    "int": INTEGER,
    "int64": INTEGER,
    "int32": INTEGER,
    "int16": INTEGER,
    "int8": INTEGER,
    "uint64": INTEGER,
    "uint32": INTEGER,
    "uint16": INTEGER,
    "uint8": INTEGER,
    "decimal": DECIMAL,
    "date": DATE,
    "timestamp": DATETIME,
    "float": FLOAT,
    "double": FLOAT,
    "float64": FLOAT,
    "float32": FLOAT,
    "string": VARCHAR,
    "array": ARRAY,
    "map": MAP,
    "json": JSON,
    "varchar": VARCHAR,
    "boolean": BOOLEAN,
    "binary": BINARY,
}


class DatabendIdentifierPreparer(PGIdentifierPreparer):
    def quote_identifier(self, value):
        """Never quote identifiers."""
        return self._escape_identifier(value)

    def quote(self, ident, force=None):
        if self._requires_quotes(ident):
            return '"{}"'.format(ident)
        return ident


class DatabendCompiler(PGCompiler):
    def visit_count_func(self, fn, **kw):
        return "count{0}".format(self.process(fn.clause_expr, **kw))

    def visit_random_func(self, fn, **kw):
        return "rand()"

    def visit_now_func(self, fn, **kw):
        return "now()"

    def visit_current_date_func(self, fn, **kw):
        return "today()"

    def visit_true(self, element, **kw):
        return "1"

    def visit_false(self, element, **kw):
        return "0"

    def visit_cast(self, cast, **kwargs):
        if self.dialect.supports_cast:
            return super(DatabendCompiler, self).visit_cast(cast, **kwargs)
        else:
            return self.process(cast.clause, **kwargs)

    def visit_substring_func(self, func, **kw):
        s = self.process(func.clauses.clauses[0], **kw)
        start = self.process(func.clauses.clauses[1], **kw)
        if len(func.clauses.clauses) > 2:
            length = self.process(func.clauses.clauses[2], **kw)
            return "substring(%s, %s, %s)" % (s, start, length)
        else:
            return "substring(%s, %s)" % (s, start)

    def visit_concat_op_binary(self, binary, operator, **kw):
        return "concat(%s, %s)" % (
            self.process(binary.left),
            self.process(binary.right),
        )

    def visit_in_op_binary(self, binary, operator, **kw):
        kw["literal_binds"] = True
        return "%s IN %s" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw),
        )

    def visit_notin_op_binary(self, binary, operator, **kw):
        kw["literal_binds"] = True
        return "%s NOT IN %s" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw),
        )

    def visit_column(
            self, column, add_to_result_map=None, include_table=True, **kwargs
    ):
        # Columns prefixed with table name are not supported
        return super(DatabendCompiler, self).visit_column(
            column, add_to_result_map=add_to_result_map, include_table=False, **kwargs
        )

    def render_literal_value(self, value, type_):
        value = super(DatabendCompiler, self).render_literal_value(value, type_)
        if isinstance(type_, sqltypes.DateTime):
            value = "toDateTime(%s)" % value
        if isinstance(type_, sqltypes.Date):
            value = "toDate(%s)" % value
        return value

    def limit_clause(self, select, **kw):
        text = ""
        if select._limit_clause is not None:
            text += "\n LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            text = "\n LIMIT "
            if select._limit_clause is None:
                text += self.process(sql.literal(-1))
            else:
                text += "0"
            text += "," + self.process(select._offset_clause, **kw)
        return text

    def for_update_clause(self, select, **kw):
        return ""  # Not supported


class DatabendExecutionContext(default.DefaultExecutionContext):
    @sa_util.memoized_property
    def should_autocommit(self):
        return False  # No DML supported, never autocommit


class DatabendTypeCompiler(compiler.GenericTypeCompiler):
    def visit_ARRAY(self, type, **kw):
        return "Array(%s)" % type

    def Visit_MAP(self, type, **kw):
        return "Map(%s)" % type


class DatabendDialect(default.DefaultDialect):
    name = "databend"
    driver = "databend"
    supports_cast = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_native_boolean = True
    supports_alter = True

    max_identifier_length = 127
    default_paramstyle = "pyformat"
    colspecs = colspecs
    ischema_names = ischema_names
    convert_unicode = True
    returns_unicode_strings = True
    description_encoding = None
    postfetch_lastrowid = False

    preparer = DatabendIdentifierPreparer
    type_compiler = DatabendTypeCompiler
    statement_compiler = DatabendCompiler
    execution_ctx_cls = DatabendExecutionContext

    # Required for PG-based compiler
    _backslash_escapes = True

    def __init__(
            self, context: Optional[ExecutionContext] = None, *args: Any, **kwargs: Any
    ):
        super(DatabendDialect, self).__init__(*args, **kwargs)
        self.context: Union[ExecutionContext, Dict] = context or {}

    @classmethod
    def dbapi(cls):
        try:
            import databend_sqlalchemy.connector as connector
        except Exception:
            import connector
        return connector

    def initialize(self, connection):
        pass

    def connect(self, *cargs, **cparams):
        # inherits the docstring from interfaces.Dialect.connect
        return self.dbapi.connect(*cargs, **cparams)

    def create_connect_args(self, url):
        parameters = dict(url.query)
        kwargs = {
            "dsn": "databend://%s:%s@%s:%d/%s"
                   % (url.username, url.password, url.host, url.port or 8000, url.database),
        }
        for k, v in parameters.items():
            kwargs["dsn"] = kwargs["dsn"] + "?" + k + "=" + v

        return ([], kwargs)

    def _get_default_schema_name(self, connection):
        return connection.scalar(text("select currentDatabase()"))

    def get_schema_names(self, connection, **kw):
        return [row[0] for row in connection.execute("SHOW DATABASES")]

    def get_view_names(self, connection, schema=None, **kw):
        return self.get_table_names(connection, schema, **kw)

    def _get_table_columns(self, connection, table_name, schema):
        full_table = table_name
        if schema:
            full_table = schema + "." + table_name
        # This needs the table name to be unescaped (no backticks).
        return connection.execute(text("DESC {}".format(full_table))).fetchall()

    def has_table(self, connection, table_name, schema=None, **kw):
        full_table = table_name
        if schema:
            full_table = schema + "." + table_name
        for r in connection.execute(text("EXISTS TABLE {}".format(full_table))):
            if r[0] == 1:
                return True
        return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        query = """
            select column_name, data_type, is_nullable
            from information_schema.columns
            where table_name = '{table_name}'
        """.format(
            table_name=table_name
        )

        if schema:
            query = "{query} and table_schema = '{schema}'".format(
                query=query, schema=schema
            )

        result = connection.execute(text(query))

        return [
            {
                "name": row[0],
                "type": ischema_names[extract_nullable_string(row[1]).lower()],
                "nullable": get_is_nullable(row[2]),
                "default": None,
            }
            for row in result
        ]

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # No support for foreign keys.
        return []

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # No support for primary keys.
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    # @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        query = "select table_name from information_schema.tables"
        if schema:
            query = "{query} where table_schema = '{schema}'".format(
                query=query, schema=schema
            )

        result = connection.execute(text(query))
        return [row[0] for row in result]

    def do_rollback(self, dbapi_connection):
        # No transactions
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # We decode everything as UTF-8
        return True

    def _check_unicode_description(self, connection):
        # We decode everything as UTF-8
        return True


dialect = DatabendDialect


def get_is_nullable(column_is_nullable: str) -> bool:
    return column_is_nullable == "YES"


def extract_nullable_string(target):
    pattern = r'Nullable\((\w+)(?:\((.*?)\))?\)'
    if "Nullable" in target:
        match = re.match(pattern, target)
        if match:
            return match.group(1)
        else:
            return ""
    else:
        sl = target.split("(")
        if len(sl) > 0:
            return sl[0]
        return target
