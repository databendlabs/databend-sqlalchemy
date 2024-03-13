#!/usr/bin/env python
#
# Note: parts of the file come from https://github.com/snowflakedb/snowflake-sqlalchemy
#       licensed under the same Apache 2.0 License
import decimal
import re
import datetime
import sqlalchemy.types as sqltypes
from typing import Any, Dict, Optional, Union
from sqlalchemy import util as sa_util
from sqlalchemy.engine import reflection
from sqlalchemy.sql import compiler, text, bindparam
from sqlalchemy.dialects.postgresql.base import PGCompiler, PGIdentifierPreparer
from sqlalchemy.types import (
    BIGINT,
    INTEGER,
    SMALLINT,
    DECIMAL,
    NUMERIC,
    VARCHAR,
    BINARY,
    BOOLEAN,
    FLOAT,
    JSON,
    CHAR,
    TIMESTAMP,
)
from sqlalchemy.engine import ExecutionContext, default


# Type decorators
class ARRAY(sqltypes.TypeEngine):
    __visit_name__ = "ARRAY"


class MAP(sqltypes.TypeEngine):
    __visit_name__ = "MAP"

    def __init__(self, key_type, value_type):
        self.key_type = key_type
        self.value_type = value_type
        super(MAP, self).__init__()


class DatabendDate(sqltypes.DATE):
    __visit_name__ = "DATE"

    _reg = re.compile(r"(\d+)-(\d+)-(\d+)")

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                m = self._reg.match(value)
                if not m:
                    raise ValueError(
                        "could not parse %r as a date value" % (value,)
                    )
                return datetime.date(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process


class DatabendDateTime(sqltypes.DATETIME):
    __visit_name__ = "DATETIME"

    _reg = re.compile(r"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)")

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                m = self._reg.match(value)
                if not m:
                    raise ValueError(
                        "could not parse %r as a datetime value" % (value,)
                    )
                return datetime.datetime(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process


class DatabendNumeric(sqltypes.Numeric):
    def result_processor(self, dialect, type_):

        orig = super().result_processor(dialect, type_)

        def process(value):
            if value is not None:
                if self.decimal_return_scale:
                    value = decimal.Decimal(f'{value:.{self.decimal_return_scale}f}')
                else:
                    value = decimal.Decimal(value)
            if orig:
                return orig(value)
            return value

        return process


# Type converters
ischema_names = {
    "bigint": BIGINT,
    "int": INTEGER,
    "smallint": SMALLINT,
    "tinyint": SMALLINT,
    "int64": BIGINT,
    "int32": INTEGER,
    "int16": SMALLINT,
    "int8": SMALLINT,
    "uint64": BIGINT,
    "uint32": INTEGER,
    "uint16": SMALLINT,
    "uint8": SMALLINT,
    "numeric": NUMERIC,
    "decimal": DECIMAL,
    "date": DatabendDate,
    "datetime": DatabendDateTime,
    "timestamp": DatabendDateTime,
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

# Column spec
colspecs = {
    sqltypes.Date: DatabendDate,
    sqltypes.DateTime: DatabendDateTime,
    sqltypes.DECIMAL: DatabendNumeric,
    sqltypes.Numeric: DatabendNumeric,
}


class DatabendIdentifierPreparer(PGIdentifierPreparer):
    pass


class DatabendCompiler(PGCompiler):
    def get_select_precolumns(self, select, **kw):
        # call the base implementation because Databend doesn't support DISTINCT ON
        return super(PGCompiler, self).get_select_precolumns(select, **kw)

    def visit_count_func(self, fn, **kw):
        return "count{0}".format(self.process(fn.clause_expr, **kw))

    def visit_random_func(self, fn, **kw):
        return "rand()"

    def visit_now_func(self, fn, **kw):
        return "now()"

    def visit_current_date_func(self, fn, **kw):
        return "today()"

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
            text += " \n LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            if select._limit_clause is None:
                text += "\n"
            text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text

    def for_update_clause(self, select, **kw):
        return ""  # Not supported

    def visit_like_op_binary(self, binary, operator, **kw):
        # escape = binary.modifiers.get("escape", None)
        return "%s LIKE %s" % (
            binary.left._compiler_dispatch(self, **kw),
            binary.right._compiler_dispatch(self, **kw),
        # ToDo - escape not yet supported
        # ) + (
        #     " ESCAPE " + self.render_literal_value(escape, sqltypes.STRINGTYPE)
        #     if escape
        #     else ""
        )

    def visit_not_like_op_binary(self, binary, operator, **kw):
        # escape = binary.modifiers.get("escape", None)
        return "%s NOT LIKE %s" % (
            binary.left._compiler_dispatch(self, **kw),
            binary.right._compiler_dispatch(self, **kw),
        # ToDo - escape not yet supported
        # ) + (
        #     " ESCAPE " + self.render_literal_value(escape, sqltypes.STRINGTYPE)
        #     if escape
        #     else ""
        )


class DatabendExecutionContext(default.DefaultExecutionContext):
    @sa_util.memoized_property
    def should_autocommit(self):
        return False  # No DML supported, never autocommit


class DatabendTypeCompiler(compiler.GenericTypeCompiler):
    def visit_ARRAY(self, type_, **kw):
        return "Array(%s)" % type_

    def Visit_MAP(self, type_, **kw):
        return "Map(%s)" % type_

    def visit_NUMERIC(self, type_, **kw):
        if type_.precision is None:
            return self.visit_DECIMAL(sqltypes.DECIMAL(38, 10), **kw)
        if type_.scale is None:
            return self.visit_DECIMAL(sqltypes.DECIMAL(38, 10), **kw)
        return self.visit_DECIMAL(type_, **kw)

    def visit_NVARCHAR(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)


class DatabendDDLCompiler(compiler.DDLCompiler):

    def visit_primary_key_constraint(self, constraint, **kw):
        return ""

    def visit_foreign_key_constraint(self, constraint, **kw):
        return ""

    def create_table_constraints(
        self, table, _include_foreign_key_constraints=None, **kw
    ):
        return ""

    def visit_create_index(self, create, include_schema=False, include_table_schema=True, **kw):
        return ""

    def visit_drop_index(self, drop, **kw):
        return ""


class DatabendDialect(default.DefaultDialect):
    name = "databend"
    driver = "databend"
    supports_cast = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_native_boolean = True
    supports_native_decimal = True
    supports_alter = True
    supports_comments = False
    supports_empty_insert = False
    supports_is_distinct_from = False

    supports_statement_cache = False

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
    ddl_compiler = DatabendDDLCompiler
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

    def _get_server_version_info(self, connection):
        val = connection.scalar(text("SELECT VERSION()"))
        m = re.match(r"(?:.*)v(\d+).(\d+).(\d+)-([^\(]+)(?:\()", val)
        if not m:
            raise AssertionError(
                "Could not determine version from string '%s'" % val
            )
        return tuple(int(x) for x in m.group(1, 2, 3) if x is not None)

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
        return connection.scalar(text("SELECT currentDatabase()"))

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        return [row[0] for row in connection.execute(text("SHOW DATABASES"))]

    def _get_table_columns(self, connection, table_name, schema):
        full_table = self.identifier_preparer.quote_identifier(table_name)
        if schema:
            full_table = self.identifier_preparer.quote_identifier(schema) + "." + full_table
        # This needs the table name to be unescaped (no backticks).
        return connection.execute(text(f"DESC {full_table}")).fetchall()

    def has_table(self, connection, table_name, schema=None, **kw):
        table_name = self.identifier_preparer.quote_identifier(table_name)
        if schema:
            schema = self.identifier_preparer.quote_identifier(schema)
            query = f"""EXISTS TABLE {schema}.{table_name}"""
        else:
            query = f"""EXISTS TABLE {table_name}"""

        r = connection.scalar(text(query))
        if r == 1:
            return True
        return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        query = text(
            """
            select column_name, column_type, is_nullable
            from information_schema.columns
            where table_name = :table_name
            and table_schema = :schema_name
            """
        ).bindparams(
            bindparam("table_name", type_=sqltypes.UnicodeText),
            bindparam("schema_name", type_=sqltypes.Unicode)
        )
        if schema is None:
            schema = self.default_schema_name
        result = connection.execute(query, dict(table_name=table_name, schema_name=schema))

        return [
            {
                "name": row[0],
                "type": self._get_column_type(row[1]),
                "nullable": get_is_nullable(row[2]),
                "default": None,
            }
            for row in result
        ]

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        view_name = self.identifier_preparer.quote_identifier(view_name)
        if schema:
            schema = self.identifier_preparer.quote_identifier(schema)
            query = f"""SHOW CREATE TABLE {schema}.{view_name}"""
        else:
            query = f"""SHOW CREATE TABLE {view_name}"""

        view_def = connection.execute(text(query)).first()
        return view_def[1]

    def _get_column_type(self, column_type):
        pattern = r'(?:Nullable)*(?:\()*(\w+)(?:\((.*?)\))?(?:\))*'
        match = re.match(pattern, column_type)
        if match:
            type_str = match.group(1).lower()
            charlen = match.group(2)
            args = ()
            kwargs = {}
            if type_str == 'decimal':
                if charlen:
                    # e.g.'18, 5'
                    prec, scale = charlen.split(", ")
                    args = (int(prec), int(scale))
            elif charlen:
                args = (int(charlen),)

            coltype = self.ischema_names[type_str]
            return coltype(*args, **kwargs)
        else:
            return None

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

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        query = text("""
            select table_name
            from information_schema.tables
            where table_schema = :schema_name
            and engine NOT LIKE '%VIEW%'
            """
        ).bindparams(
            bindparam("schema_name", type_=sqltypes.Unicode)
        )
        if schema is None:
            schema = self.default_schema_name

        result = connection.execute(query, dict(schema_name=schema))
        return [row[0] for row in result]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        query = text(
            """
            select table_name
            from information_schema.tables
            where table_schema = :schema_name
            and engine LIKE '%VIEW%'
            """
        ).bindparams(
            bindparam("schema_name", type_=sqltypes.Unicode)
        )
        if schema is None:
            schema = self.default_schema_name

        result = connection.execute(query, dict(schema_name=schema))
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
