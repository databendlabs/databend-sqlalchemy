#!/usr/bin/env python
#
# Note: parts of the file come from https://github.com/snowflakedb/snowflake-sqlalchemy
#       licensed under the same Apache 2.0 License

"""
Databend Table Options
------------------------

Several options for CREATE TABLE are supported directly by the Databend
dialect in conjunction with the :class:`_schema.Table` construct:

* ``ENGINE``::

    Table("some_table", metadata, ..., databend_engine=FUSE|Memory|Random|Iceberg|Delta)

* ``CLUSTER KEY``::

    Table("some_table", metadata, ..., databend_cluster_by=str|LIST(expr|str))

* ``TRANSIENT``::

    Table("some_table", metadata, ..., databend_transient=True|False)

"""
import decimal
import re
import operator
import datetime
from types import NoneType

import sqlalchemy.engine.reflection
import sqlalchemy.types as sqltypes
from typing import Any, Dict, Optional, Union
from sqlalchemy import util as sa_util
from sqlalchemy.engine import reflection
from sqlalchemy.sql import (
    compiler,
    text,
    bindparam,
    select,
    TableClause,
    Select,
    Subquery,
)
from sqlalchemy.dialects.postgresql.base import PGCompiler, PGIdentifierPreparer
from sqlalchemy import Table, MetaData, Column
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

import sqlalchemy
from sqlalchemy import types as sqltypes
from sqlalchemy.sql.base import Executable

# Check SQLAlchemy version
if sqlalchemy.__version__.startswith('2.'):
    from sqlalchemy.types import DOUBLE
else:
    from .types import DOUBLE

from sqlalchemy.engine import ExecutionContext, default
from sqlalchemy.exc import DBAPIError, NoSuchTableError

from .dml import (
    Merge,
    StageClause,
    _StorageClause,
    GoogleCloudStorage,
    AzureBlobStorage,
    AmazonS3,
)
from .types import INTERVAL, TINYINT, BITMAP, GEOMETRY, GEOGRAPHY

RESERVED_WORDS = {
    "Error",
    "EOI",
    "Whitespace",
    "Comment",
    "CommentBlock",
    "Ident",
    "IdentVariable",
    "ColumnPosition",
    "LiteralString",
    "LiteralCodeString",
    "LiteralAtString",
    "PGLiteralHex",
    "MySQLLiteralHex",
    "LiteralInteger",
    "LiteralFloat",
    "HintPrefix",
    "HintSuffix",
    "DoubleEq",
    "Eq",
    "NotEq",
    "Lt",
    "Gt",
    "Lte",
    "Gte",
    "Spaceship",
    "Plus",
    "Minus",
    "Multiply",
    "Divide",
    "IntDiv",
    "Modulo",
    "StringConcat",
    "LParen",
    "RParen",
    "Comma",
    "Dot",
    "Colon",
    "DoubleColon",
    "ColonEqual",
    "SemiColon",
    "Backslash",
    "LBracket",
    "RBracket",
    "Caret",
    "LBrace",
    "RBrace",
    "Dollar",
    "RArrow",
    "LongRArrow",
    "FatRArrow",
    "HashRArrow",
    "HashLongRArrow",
    "TildeAsterisk",
    "ExclamationMarkTilde",
    "ExclamationMarkTildeAsterisk",
    "BitWiseAnd",
    "BitWiseOr",
    "BitWiseXor",
    "BitWiseNot",
    "ShiftLeft",
    "ShiftRight",
    "Factorial",
    "DoubleExclamationMark",
    "Abs",
    "SquareRoot",
    "CubeRoot",
    "Placeholder",
    "QuestionOr",
    "QuestionAnd",
    "ArrowAt",
    "AtArrow",
    "AtQuestion",
    "AtAt",
    "HashMinus",
    "ACCOUNT",
    "ALL",
    "ALLOWED_IP_LIST",
    "ADD",
    "AFTER",
    "AGGREGATING",
    "ANY",
    "APPEND_ONLY",
    "ARGS",
    "AUTO",
    "SOME",
    "ALTER",
    "ALWAYS",
    "ANALYZE",
    "AND",
    "ARRAY",
    "AS",
    "AST",
    "AT",
    "ASC",
    "ANTI",
    "ASYNC",
    "ATTACH",
    "BEFORE",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BREAK",
    "LONGBLOB",
    "MEDIUMBLOB",
    "TINYBLOB",
    "BLOB",
    "BINARY_FORMAT",
    "BITMAP",
    "BLOCKED_IP_LIST",
    "BOOL",
    "BOOLEAN",
    "BOTH",
    "BY",
    "BROTLI",
    "BZ2",
    "BLOCK",
    "CALL",
    "CASE",
    "CASE_SENSITIVE",
    "CAST",
    "CATALOG",
    "CATALOGS",
    "CENTURY",
    "CHANGES",
    "CLUSTER",
    "COMMENT",
    "COMMENTS",
    "COMPACT",
    "CONNECTION",
    "CONNECTIONS",
    "CONSUME",
    "CONTENT_TYPE",
    "CONTINUE",
    "CHAR",
    "COLUMN",
    "COLUMN_MATCH_MODE",
    "COLUMNS",
    "CHARACTER",
    "CONFLICT",
    "COMPRESSION",
    "COPY_OPTIONS",
    "COPY",
    "COUNT",
    "CREDENTIAL",
    "CREATE",
    "CROSS",
    "CSV",
    "CURRENT",
    "CURRENT_TIMESTAMP",
    "DATABASE",
    "DATABASES",
    "DATA",
    "DATE",
    "DATE_ADD",
    "DATE_DIFF",
    "DATE_PART",
    "DATE_SUB",
    "DATE_TRUNC",
    "DATETIME",
    "DAY",
    "DECADE",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DEFLATE",
    "DELETE",
    "DESC",
    "DETAILED_OUTPUT",
    "DESCRIBE",
    "DISABLE",
    "DISABLE_VARIANT_CHECK",
    "DISTINCT",
    "RESPECT",
    "IGNORE",
    "DIV",
    "DOUBLE_SHA1_PASSWORD",
    "DO",
    "DOUBLE",
    "DOW",
    "WEEK",
    "DELTA",
    "DOY",
    "DOWNLOAD",
    "DOWNSTREAM",
    "DROP",
    "DRY",
    "DYNAMIC",
    "EXCEPT",
    "EXCLUDE",
    "ELSE",
    "EMPTY_FIELD_AS",
    "ENABLE",
    "ENABLE_VIRTUAL_HOST_STYLE",
    "END",
    "ENDPOINT",
    "ENGINE",
    "ENGINES",
    "EPOCH",
    "MICROSECOND",
    "ERROR_ON_COLUMN_COUNT_MISMATCH",
    "ESCAPE",
    "EXCEPTION_BACKTRACE",
    "EXISTS",
    "EXPLAIN",
    "EXPIRE",
    "EXTRACT",
    "ELSEIF",
    "FALSE",
    "FIELDS",
    "FIELD_DELIMITER",
    "NAN_DISPLAY",
    "NULL_DISPLAY",
    "NULL_IF",
    "FILE_FORMAT",
    "FILE",
    "FILES",
    "FINAL",
    "FLASHBACK",
    "FLOAT",
    "FLOAT32",
    "FLOAT64",
    "FOR",
    "FORCE",
    "FORMAT",
    "FOLLOWING",
    "FORMAT_NAME",
    "FORMATS",
    "FRAGMENTS",
    "FRIDAY",
    "FROM",
    "FULL",
    "FUNCTION",
    "FUNCTIONS",
    "TABLE_FUNCTIONS",
    "SET_VAR",
    "FUSE",
    "GET",
    "GENERATED",
    "GEOMETRY",
    "GEOGRAPHY",
    "GLOBAL",
    "GRAPH",
    "GROUP",
    "GZIP",
    "HAVING",
    "HIGH",
    "HILBERT",
    "HISTORY",
    "HIVE",
    "HOUR",
    "HOURS",
    "ICEBERG",
    "INTERSECT",
    "IDENTIFIED",
    "IDENTIFIER",
    "IF",
    "IN",
    "INCLUDE_QUERY_ID",
    "INCREMENTAL",
    "INDEX",
    "INFORMATION",
    "INITIALIZE",
    "INNER",
    "INSERT",
    "INT",
    "INT16",
    "INT32",
    "INT64",
    "INT8",
    "INTEGER",
    "INTERVAL",
    "INTO",
    "INVERTED",
    "PREVIOUS_DAY",
    "PROCEDURE",
    "PROCEDURES",
    "IMMEDIATE",
    "IS",
    "ISODOW",
    "ISOYEAR",
    "JOIN",
    "JSON",
    "JULIAN",
    "JWT",
    "KEY",
    "KILL",
    "LAST_DAY",
    "LATERAL",
    "LINEAR",
    "LOCATION_PREFIX",
    "LOCKS",
    "LOGICAL",
    "LOOP",
    "SECONDARY",
    "ROLES",
    "L2DISTANCE",
    "LEADING",
    "LEFT",
    "LET",
    "LIKE",
    "LIMIT",
    "LIST",
    "LOW",
    "LZO",
    "MASKING",
    "MAP",
    "MAX_FILE_SIZE",
    "MASTER_KEY",
    "MEDIUM",
    "MEMO",
    "MEMORY",
    "METRICS",
    "MICROSECONDS",
    "MILLENNIUM",
    "MILLISECONDS",
    "MINUTE",
    "MONTH",
    "MODIFY",
    "MATERIALIZED",
    "MUST_CHANGE_PASSWORD",
    "NEXT_DAY",
    "NON_DISPLAY",
    "NATURAL",
    "NETWORK",
    "DISABLED",
    "NDJSON",
    "NO_PASSWORD",
    "NONE",
    "NOT",
    "NOTENANTSETTING",
    "DEFAULT_ROLE",
    "NULL",
    "NULLABLE",
    "OBJECT",
    "OF",
    "OFFSET",
    "ON",
    "ON_CREATE",
    "ON_SCHEDULE",
    "OPTIMIZE",
    "OPTIONS",
    "OR",
    "ORC",
    "ORDER",
    "OUTPUT_HEADER",
    "OUTER",
    "ON_ERROR",
    "OVER",
    "OVERWRITE",
    "PARTITION",
    "PARQUET",
    "PASSWORD",
    "PASSWORD_MIN_LENGTH",
    "PASSWORD_MAX_LENGTH",
    "PASSWORD_MIN_UPPER_CASE_CHARS",
    "PASSWORD_MIN_LOWER_CASE_CHARS",
    "PASSWORD_MIN_NUMERIC_CHARS",
    "PASSWORD_MIN_SPECIAL_CHARS",
    "PASSWORD_MIN_AGE_DAYS",
    "PASSWORD_MAX_AGE_DAYS",
    "PASSWORD_MAX_RETRIES",
    "PASSWORD_LOCKOUT_TIME_MINS",
    "PASSWORD_HISTORY",
    "PATTERN",
    "PIPELINE",
    "PLAINTEXT_PASSWORD",
    "POLICIES",
    "POLICY",
    "POSITION",
    "PROCESSLIST",
    "PRIORITY",
    "PURGE",
    "PUT",
    "PARTIAL",
    "QUARTER",
    "QUERY",
    "QUOTE",
    "RANGE",
    "RAWDEFLATE",
    "READ_ONLY",
    "RECLUSTER",
    "RECORD_DELIMITER",
    "REFERENCE_USAGE",
    "REFRESH",
    "REGEXP",
    "RENAME",
    "REPLACE",
    "RETURN_FAILED_ONLY",
    "REVERSE",
    "SAMPLE",
    "MERGE",
    "MATCHED",
    "MISSING_FIELD_AS",
    "NULL_FIELD_AS",
    "UNMATCHED",
    "ROW",
    "ROWS",
    "ROW_TAG",
    "GRANT",
    "REPEAT",
    "ROLE",
    "PRECEDING",
    "PRECISION",
    "PRESIGN",
    "PRIVILEGES",
    "QUALIFY",
    "REMOVE",
    "RETAIN",
    "REVOKE",
    "RECURSIVE",
    "RETURN",
    "RETURNS",
    "RESULTSET",
    "RUN",
    "GRANTS",
    "REFRESH_MODE",
    "RIGHT",
    "RLIKE",
    "RAW",
    "OPTIMIZED",
    "DECORRELATED",
    "SATURDAY",
    "SCHEMA",
    "SCHEMAS",
    "SECOND",
    "MILLISECOND",
    "SELECT",
    "PIVOT",
    "UNPIVOT",
    "SEGMENT",
    "SET",
    "UNSET",
    "SESSION",
    "SETTINGS",
    "VARIABLES",
    "STAGES",
    "STATISTIC",
    "SUMMARY",
    "SHA256_PASSWORD",
    "SHOW",
    "SINCE",
    "SIGNED",
    "SINGLE",
    "SIZE_LIMIT",
    "MAX_FILES",
    "MONDAY",
    "SKIP_HEADER",
    "SMALLINT",
    "SNAPPY",
    "SNAPSHOT",
    "SPLIT_SIZE",
    "STAGE",
    "SYNTAX",
    "USAGE",
    "USE_RAW_PATH",
    "UPDATE",
    "UPLOAD",
    "SEQUENCE",
    "SHARE",
    "SHARES",
    "SUPER",
    "STATUS",
    "STORED",
    "STREAM",
    "STREAMS",
    "STRING",
    "SUBSTRING",
    "SUBSTR",
    "SEMI",
    "SOUNDS",
    "SYNC",
    "SYSTEM",
    "STORAGE_TYPE",
    "TABLE",
    "TABLES",
    "TARGET_LAG",
    "TEXT",
    "LONGTEXT",
    "MEDIUMTEXT",
    "TINYTEXT",
    "TENANTSETTING",
    "TENANTS",
    "TENANT",
    "THEN",
    "THURSDAY",
    "TIMESTAMP",
    "TIMEZONE_HOUR",
    "TIMEZONE_MINUTE",
    "TIMEZONE",
    "TINYINT",
    "TO",
    "TOKEN",
    "TRAILING",
    "TRANSIENT",
    "TRIM",
    "TRUE",
    "TRUNCATE",
    "TRY_CAST",
    "TSV",
    "TUESDAY",
    "TUPLE",
    "TYPE",
    "UNBOUNDED",
    "UNION",
    "UINT16",
    "UINT32",
    "UINT64",
    "UINT8",
    "UNDROP",
    "UNSIGNED",
    "URL",
    "METHOD",
    "AUTHORIZATION_HEADER",
    "USE",
    "USER",
    "USERS",
    "USING",
    "VACUUM",
    "VALUES",
    "VARBINARY",
    "VARCHAR",
    "VARIANT",
    "VARIABLE",
    "VERBOSE",
    "GRAPHICAL",
    "VIEW",
    "VIEWS",
    "VIRTUAL",
    "WHEN",
    "WHERE",
    "WHILE",
    "WINDOW",
    "WITH",
    "XML",
    "XOR",
    "XZ",
    "YEAR",
    "ZSTD",
    "NULLIF",
    "COALESCE",
    "RANDOM",
    "IFNULL",
    "NULLS",
    "FIRST",
    "LAST",
    "IGNORE_RESULT",
    "GROUPING",
    "SETS",
    "CUBE",
    "ROLLUP",
    "INDEXES",
    "ADDRESS",
    "OWNERSHIP",
    "READ",
    "WRITE",
    "UDF",
    "HANDLER",
    "LANGUAGE",
    "STATE",
    "TASK",
    "TASKS",
    "TOP",
    "WAREHOUSE",
    "SCHEDULE",
    "SUSPEND_TASK_AFTER_NUM_FAILURES",
    "CRON",
    "EXECUTE",
    "SUSPEND",
    "RESUME",
    "PIPE",
    "NOTIFICATION",
    "INTEGRATION",
    "ENABLED",
    "WEBHOOK",
    "WEDNESDAY",
    "ERROR_INTEGRATION",
    "AUTO_INGEST",
    "PIPE_EXECUTION_PAUSED",
    "PREFIX",
    "MODIFIED_AFTER",
    "UNTIL",
    "BEGIN",
    "TRANSACTION",
    "COMMIT",
    "ABORT",
    "ROLLBACK",
    "TEMPORARY",
    "TEMP",
    "SECONDS",
    "DAYS",
    "DICTIONARY",
    "DICTIONARIES",
    "PRIMARY",
    "SOURCE",
    "SQL",
    "SUNDAY",
    "WAREHOUSES",
    "INSPECT",
    "ASSIGN",
    "NODES",
    "UNASSIGN",
    "ONLINE",
}


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
                    raise ValueError("could not parse %r as a date value" % (value,))
                return datetime.date(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process


class DatabendDateTime(sqltypes.DATETIME):
    __visit_name__ = "DATETIME"

    _reg = re.compile(r"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)\.(\d+)")

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

    def literal_processor(self, dialect):
        def process(value):
            if value is not None:
                datetime_str = value.isoformat(" ", timespec="microseconds")
                return f"'{datetime_str}'"

        return process


class DatabendTime(sqltypes.TIME):
    __visit_name__ = "TIME"

    _reg = re.compile(r"(?:\d+)-(?:\d+)-(?:\d+) (\d+):(\d+):(\d+)\.(\d+)")

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            if isinstance(value, str):
                m = self._reg.match(value)
                if not m:
                    raise ValueError(
                        "could not parse %r as a datetime value" % (value,)
                    )
                return datetime.time(*[int(x or 0) for x in m.groups()])
            else:
                return value.time()

        return process

    def literal_processor(self, dialect):
        def process(value):
            if value is not None:
                from_min_value = datetime.datetime.combine(
                    datetime.date(1970, 1, 1), value
                )
                time_str = from_min_value.isoformat(timespec="microseconds")
                return f"'{time_str}'"

        return process


class DatabendNumeric(sqltypes.Numeric):
    def result_processor(self, dialect, type_):
        orig = super().result_processor(dialect, type_)

        def process(value):
            if value is not None:
                if self.decimal_return_scale:
                    value = decimal.Decimal(f"{value:.{self.decimal_return_scale}f}")
                else:
                    value = decimal.Decimal(value)
            if orig:
                return orig(value)
            return value

        return process


class DatabendInterval(INTERVAL):
    render_bind_cast = True


class DatabendBitmap(BITMAP):
    render_bind_cast = True


class DatabendTinyInt(TINYINT):
    render_bind_cast = True


class DatabendGeometry(GEOMETRY):
    render_bind_cast = True

class DatabendGeography(GEOGRAPHY):
    render_bind_cast = True

# Type converters
ischema_names = {
    "bigint": BIGINT,
    "int": INTEGER,
    "smallint": SMALLINT,
    "tinyint": DatabendTinyInt,
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
    "double": DOUBLE,
    "float64": FLOAT,
    "float32": FLOAT,
    "string": VARCHAR,
    "array": ARRAY,
    "map": MAP,
    "json": JSON,
    "variant": JSON,
    "varchar": VARCHAR,
    "boolean": BOOLEAN,
    "binary": BINARY,
    "time": DatabendTime,
    "interval": DatabendInterval,
    "bitmap": DatabendBitmap,
    "geometry": DatabendGeometry,
    "geography": DatabendGeography
}



# Column spec
colspecs = {
    sqltypes.Interval: DatabendInterval,
    sqltypes.Time: DatabendTime,
    sqltypes.Date: DatabendDate,
    sqltypes.DateTime: DatabendDateTime,
    sqltypes.DECIMAL: DatabendNumeric,
    sqltypes.Numeric: DatabendNumeric,
}


class DatabendIdentifierPreparer(PGIdentifierPreparer):
    reserved_words = {r.lower() for r in RESERVED_WORDS}


class DatabendCompiler(PGCompiler):
    iscopyintotable: bool = False
    iscopyintolocation: bool = False

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
        # if isinstance(type_, sqltypes.DateTime):
        #     return "to_datetime(%s)" % value
        # if isinstance(type_, sqltypes.Date):
        #     return "to_date(%s)" % value
        # if isinstance(type_, sqltypes.Time):
        #     return "to_datetime(%s)" % value
        # if isinstance(type_, sqltypes.Interval):
        #     return "to_datetime(%s)" % value
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

    def visit_merge(self, merge, **kw):
        clauses = "\n ".join(
            clause._compiler_dispatch(self, **kw) for clause in merge.clauses
        )
        source_kw = {"asfrom": True}
        if isinstance(merge.source, TableClause):
            source = (
                select(merge.source)
                .subquery()
                .alias(merge.source.name)
                ._compiler_dispatch(self, **source_kw)
            )
        elif isinstance(merge.source, Select):
            source = (
                merge.source.subquery()
                .alias(merge.source.get_final_froms()[0].name)
                ._compiler_dispatch(self, **source_kw)
            )
        elif isinstance(merge.source, Subquery):
            source = merge.source._compiler_dispatch(self, **source_kw)
        else:
            source = merge.source

        merge_on = merge.on._compiler_dispatch(self, **kw)

        target_table = self.preparer.format_table(merge.target)
        return (
            f"MERGE INTO {target_table}\n"
            f" USING {source}\n"
            f" ON {merge_on}\n"
            f" {clauses if clauses else ''}"
        )

    def visit_when_merge_matched_update(self, merge_matched_update, **kw):
        case_predicate = (
            f" AND {str(merge_matched_update.predicate._compiler_dispatch(self, **kw))}"
            if merge_matched_update.predicate is not None
            else ""
        )
        update_str = f"WHEN MATCHED{case_predicate} THEN\n UPDATE"
        if not merge_matched_update.set:
            return f"{update_str} *"

        set_list = list(merge_matched_update.set.items())
        if kw.get("deterministic", False):
            set_list.sort(key=operator.itemgetter(0))
        set_values = ", ".join(
            [
                f"{self.preparer.quote_identifier(set_item[0])} = {set_item[1]._compiler_dispatch(self, **kw)}"
                for set_item in set_list
            ]
        )
        return f"{update_str} SET {str(set_values)}"

    def visit_when_merge_matched_delete(self, merge_matched_delete, **kw):
        case_predicate = (
            f" AND {str(merge_matched_delete.predicate._compiler_dispatch(self, **kw))}"
            if merge_matched_delete.predicate is not None
            else ""
        )
        return f"WHEN MATCHED{case_predicate} THEN DELETE"

    def visit_when_merge_unmatched(self, merge_unmatched, **kw):
        case_predicate = (
            f" AND {str(merge_unmatched.predicate._compiler_dispatch(self, **kw))}"
            if merge_unmatched.predicate is not None
            else ""
        )
        insert_str = f"WHEN NOT MATCHED{case_predicate} THEN\n INSERT"
        if not merge_unmatched.set:
            return f"{insert_str} *"

        set_cols, sets_vals = zip(*merge_unmatched.set.items())
        set_cols, sets_vals = list(set_cols), list(sets_vals)
        if kw.get("deterministic", False):
            set_cols, sets_vals = zip(
                *sorted(merge_unmatched.set.items(), key=operator.itemgetter(0))
            )
        return "{} ({}) VALUES ({})".format(
            insert_str,
            ", ".join(set_cols),
            ", ".join(map(lambda e: e._compiler_dispatch(self, **kw), sets_vals)),
        )

    def visit_copy_into(self, copy_into, **kw):
        if isinstance(copy_into.target, (TableClause,)):
            self.iscopyintotable = True
        else:
            self.iscopyintolocation = True

        target = (
            self.preparer.format_table(copy_into.target)
            if isinstance(copy_into.target, (TableClause,))
            else copy_into.target._compiler_dispatch(self, **kw)
        )

        if isinstance(copy_into.from_, (TableClause,)):
            source = self.preparer.format_table(copy_into.from_)
        elif isinstance(copy_into.from_, (_StorageClause, StageClause)):
            source = copy_into.from_._compiler_dispatch(self, **kw)
        # elif isinstance(copy_into.from_, (FileColumnClause)):
        #     source = f"({copy_into.from_._compiler_dispatch(self, **kw)})"
        else:
            source = f"({copy_into.from_._compiler_dispatch(self, **kw)})"

        result = f"COPY INTO {target}" f" FROM {source}"
        if hasattr(copy_into, "files") and isinstance(copy_into.files, list):
            quoted_files = [f"'{f}'" for f in copy_into.files]
            result += f" FILES = ({', '.join(quoted_files)})"
        if hasattr(copy_into, "pattern") and copy_into.pattern:
            result += f" PATTERN = '{copy_into.pattern}'"
        if not isinstance(copy_into.file_format, NoneType):
            result += f" {copy_into.file_format._compiler_dispatch(self, **kw)}\n"
        if not isinstance(copy_into.options, NoneType):
            result += f" {copy_into.options._compiler_dispatch(self, **kw)}\n"

        return result

    def visit_copy_format(self, file_format, **kw):
        options_list = list(file_format.options.items())
        if kw.get("deterministic", False):
            options_list.sort(key=operator.itemgetter(0))
        # predefined format name
        if "format_name" in file_format.options:
            return f"FILE_FORMAT=(format_name = {file_format.options['format_name']})"
        # format specifics
        format_options = [f"TYPE = {file_format.format_type}"]
        format_options.extend(
            [
                "{} = {}".format(
                    option,
                    (
                        value._compiler_dispatch(self, **kw)
                        if hasattr(value, "_compiler_dispatch")
                        else str(value)
                    ),
                )
                for option, value in options_list
            ]
        )
        return f"FILE_FORMAT = ({', '.join(format_options)})"

    def visit_copy_into_options(self, copy_into_options, **kw):
        options_list = list(copy_into_options.options.items())
        # if kw.get("deterministic", False):
        #     options_list.sort(key=operator.itemgetter(0))
        return "\n".join([f"{k} = {v}" for k, v in options_list])

    def visit_file_column(self, file_column_clause, **kw):
        if isinstance(file_column_clause.from_, (TableClause,)):
            source = self.preparer.format_table(file_column_clause.from_)
        elif isinstance(file_column_clause.from_, (_StorageClause, StageClause)):
            source = file_column_clause.from_._compiler_dispatch(self, **kw)
        else:
            source = f"({file_column_clause.from_._compiler_dispatch(self, **kw)})"
        if isinstance(file_column_clause.columns, str):
            select_str = file_column_clause.columns
        else:
            select_str = ",".join(
                [
                    col._compiler_dispatch(self, **kw)
                    for col in file_column_clause.columns
                ]
            )
        return f"SELECT {select_str}" f" FROM {source}"

    def visit_amazon_s3(self, amazon_s3: AmazonS3, **kw):
        connection_params_str = f"  ACCESS_KEY_ID = '{amazon_s3.access_key_id}' \n"
        connection_params_str += (
            f"  SECRET_ACCESS_KEY = '{amazon_s3.secret_access_key}'\n"
        )
        if amazon_s3.endpoint_url:
            connection_params_str += f"  ENDPOINT_URL = '{amazon_s3.endpoint_url}' \n"
        if amazon_s3.enable_virtual_host_style:
            connection_params_str += f"  ENABLE_VIRTUAL_HOST_STYLE = '{amazon_s3.enable_virtual_host_style}'\n"
        if amazon_s3.master_key:
            connection_params_str += f"  MASTER_KEY = '{amazon_s3.master_key}'\n"
        if amazon_s3.region:
            connection_params_str += f"  REGION = '{amazon_s3.region}'\n"
        if amazon_s3.security_token:
            connection_params_str += (
                f"  SECURITY_TOKEN = '{amazon_s3.security_token}'\n"
            )

        return (
            f"'{amazon_s3.uri}' \n"
            f"CONNECTION = (\n"
            f"{connection_params_str}\n"
            f")"
        )

    def visit_azure_blob_storage(self, azure: AzureBlobStorage, **kw):
        return (
            f"'{azure.uri}' \n"
            f"CONNECTION = (\n"
            f"  ENDPOINT_URL = 'https://{azure.account_name}.blob.core.windows.net' \n"
            f"  ACCOUNT_NAME = '{azure.account_name}' \n"
            f"  ACCOUNT_KEY = '{azure.account_key}'\n"
            f")"
        )

    def visit_google_cloud_storage(self, gcs: GoogleCloudStorage, **kw):
        return (
            f"'{gcs.uri}' \n"
            f"CONNECTION = (\n"
            f"  ENDPOINT_URL = 'https://storage.googleapis.com' \n"
            f"  CREDENTIAL = '{gcs.credentials}' \n"
            f")"
        )

    def visit_stage(self, stage, **kw):
        if stage.path:
            return f"@{stage.name}/{stage.path}"
        return f"@{stage.name}"


class DatabendExecutionContext(default.DefaultExecutionContext):
    iscopyintotable = False
    iscopyintolocation = False

    _copy_input_bytes: Optional[int] = None
    _copy_output_bytes: Optional[int] = None
    _copy_into_table_results: Optional[list[dict]] = None
    _copy_into_location_results: dict = None

    @sa_util.memoized_property
    def should_autocommit(self):
        return False  # No DML supported, never autocommit

    def create_server_side_cursor(self):
        return self._dbapi_connection.cursor()

    def create_default_cursor(self):
        return self._dbapi_connection.cursor()

    def post_exec(self):
        self.iscopyintotable = getattr(self.compiled, 'iscopyintotable', False)
        self.iscopyintolocation = getattr(self.compiled, 'iscopyintolocation', False)
        if (self.isinsert or self.isupdate or self.isdelete or
            self.iscopyintolocation or self.iscopyintotable):
            result = self.cursor.fetchall()
            if self.iscopyintotable:
                self._copy_into_table_results = [
                    {
                        'file': row[0],
                        'rows_loaded': row[1],
                        'errors_seen': row[2],
                        'first_error': row[3],
                        'first_error_line': row[4],
                    } for row in result
                ]
                self._rowcount = sum(c['rows_loaded'] for c in self._copy_into_table_results)
            else:
                self._rowcount = result[0][0]
                if self.iscopyintolocation:
                    self._copy_into_location_results = {
                        'rows_unloaded': result[0][0],
                        'input_bytes': result[0][1],
                        'output_bytes': result[0][2],
                    }

    def copy_into_table_results(self) -> list[dict]:
        return self._copy_into_table_results

    def copy_into_location_results(self) -> dict:
        return self._copy_into_location_results


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

    def visit_JSON(self, type_, **kw):
        return "JSON"  # or VARIANT

    def visit_TIME(self, type_, **kw):
        return "DATETIME"

    def visit_INTERVAL(self, type, **kw):
        return "INTERVAL"

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_TINYINT(self, type_, **kw):
        return "TINYINT"

    def visit_FLOAT(self, type_, **kw):
        return "FLOAT"

    def visit_BITMAP(self, type_, **kw):
        return "BITMAP"

    def visit_GEOMETRY(self, type_, **kw):
        if type_.srid is not None:
            return f"GEOMETRY(SRID {type_.srid})"
        return "GEOMETRY"

    def visit_GEOGRAPHY(self, type_, **kw):
        if type_.srid is not None:
            return f"GEOGRAPHY(SRID {type_.srid})"
        return "GEOGRAPHY"



class DatabendDDLCompiler(compiler.DDLCompiler):
    def visit_primary_key_constraint(self, constraint, **kw):
        return ""

    def visit_foreign_key_constraint(self, constraint, **kw):
        return ""

    def create_table_constraints(
        self, table, _include_foreign_key_constraints=None, **kw
    ):
        return ""

    def visit_create_index(
        self, create, include_schema=False, include_table_schema=True, **kw
    ):
        return ""

    def visit_drop_index(self, drop, **kw):
        return ""

    def visit_drop_schema(self, drop, **kw):
        # Override - Databend does not support the CASCADE option
        schema = self.preparer.format_schema(drop.element)
        return "DROP SCHEMA " + schema

    def visit_create_table(self, create, **kw):
        table = create.element
        db_opts = table.dialect_options["databend"]
        if "transient" in db_opts and db_opts["transient"]:
            if "transient" not in [p.lower() for p in table._prefixes]:
                table._prefixes.append("TRANSIENT")
        return super().visit_create_table(create, **kw)

    def post_create_table(self, table):
        table_opts = []
        db_opts = table.dialect_options["databend"]

        engine = db_opts.get("engine")
        if engine is not None:
            table_opts.append(f" ENGINE={engine}")

        if table.comment is not None:
            comment = self.sql_compiler.render_literal_value(
                table.comment, sqltypes.String()
            )
            table_opts.append(f" COMMENT={comment}")

        cluster_keys = db_opts.get("cluster_by")
        if cluster_keys is not None:
            if isinstance(cluster_keys, str):
                cluster_by = cluster_keys
            elif isinstance(cluster_keys, list):
                cluster_by = ", ".join(
                    self.sql_compiler.process(
                        expr if not isinstance(expr, str) else table.c[expr],
                        include_table=False,
                        literal_binds=True,
                    )
                    for expr in cluster_keys
                )
            else:
                cluster_by = ""
            table_opts.append(f"\n CLUSTER BY ( {cluster_by} )")

        # ToDo - Engine options

        return " ".join(table_opts)

    def get_column_specification(self, column, **kwargs):
        colspec = super().get_column_specification(column, **kwargs)
        comment = column.comment
        if comment is not None:
            literal = self.sql_compiler.render_literal_value(
                comment, sqltypes.String()
            )
            colspec += " COMMENT " + literal

        return colspec

    def visit_set_table_comment(self, create, **kw):
        return "ALTER TABLE %s COMMENT = %s" % (
            self.preparer.format_table(create.element),
            self.sql_compiler.render_literal_value(
                create.element.comment, sqltypes.String()
            ),
        )

    def visit_drop_table_comment(self, create, **kw):
        return "ALTER TABLE %s COMMENT = ''" % (
            self.preparer.format_table(create.element)
        )

    def visit_set_column_comment(self, create, **kw):
        return "ALTER TABLE %s MODIFY %s %s" % (
            self.preparer.format_table(create.element.table),
            self.preparer.format_column(create.element),
            self.get_column_specification(create.element),
        )


class DatabendDialect(default.DefaultDialect):
    name = "databend"
    driver = "databend"
    supports_cast = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_native_boolean = True
    supports_native_decimal = True
    supports_alter = True
    supports_comments = False
    supports_empty_insert = False
    supports_is_distinct_from = True
    supports_multivalues_insert = True

    supports_statement_cache = False
    supports_server_side_cursors = True

    max_identifier_length = 127
    default_paramstyle = "pyformat"
    colspecs = colspecs
    ischema_names = ischema_names
    returns_native_bytes = True
    div_is_floordiv = False
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
        self,
        context: Optional[ExecutionContext] = None,
        json_serializer=None,
        json_deserializer=None,
        *args: Any,
        **kwargs: Any,
    ):
        super(DatabendDialect, self).__init__(*args, **kwargs)
        self.context: Union[ExecutionContext, Dict] = context or {}
        self._json_serializer = json_serializer
        self._json_deserializer = json_deserializer

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()

    @classmethod
    def import_dbapi(cls):
        try:
            import databend_sqlalchemy.connector as connector
        except Exception:
            import connector
        return connector

    def _get_server_version_info(self, connection):
        val = connection.scalar(text("SELECT VERSION()"))
        m = re.match(r"(?:.*)v(\d+).(\d+).(\d+)-([^\(]+)(?:\()", val)
        if not m:
            raise AssertionError("Could not determine version from string '%s'" % val)
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

        if parameters:
            kwargs["dsn"] += "?"
            param_strings = []
            for k, v in parameters.items():
                param_strings.append(f"{k}={v}")
            kwargs["dsn"] += "&".join(param_strings)

        return ([], kwargs)

    def create_server_side_cursor(self):
        return self.create_default_cursor()

    def _get_default_schema_name(self, connection):
        return connection.scalar(text("SELECT currentDatabase()"))

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        return [row[0] for row in connection.execute(text("SHOW DATABASES"))]

    def _get_table_columns(self, connection, table_name, schema):
        if schema is None:
            schema = self.default_schema_name
        quote_table_name = self.identifier_preparer.quote_identifier(table_name)
        quote_schema = self.identifier_preparer.quote_identifier(schema)

        return connection.execute(
            text(f"DESC {quote_schema}.{quote_table_name}")
        ).fetchall()

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        if schema is None:
            schema = self.default_schema_name
        quote_table_name = self.identifier_preparer.quote_identifier(table_name)
        quote_schema = self.identifier_preparer.quote_identifier(schema)
        query = f"""EXISTS TABLE {quote_schema}.{quote_table_name}"""
        r = connection.scalar(text(query))
        if r == 1:
            return True
        return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        query = text(
            """
            select column_name, column_type, is_nullable, nullif(column_comment, '')
            from information_schema.columns
            where table_name = :table_name
            and table_schema = :schema_name
            """
        ).bindparams(
            bindparam("table_name", type_=sqltypes.UnicodeText),
            bindparam("schema_name", type_=sqltypes.Unicode),
        )
        if schema is None:
            schema = self.default_schema_name
        result = connection.execute(
            query, dict(table_name=table_name, schema_name=schema)
        )

        cols = [
            {
                "name": row[0],
                "type": self._get_column_type(row[1]),
                "nullable": get_is_nullable(row[2]),
                "default": None,
                "comment": row[3],
            }
            for row in result
        ]
        if not cols and not self.has_table(connection, table_name, schema):
            raise NoSuchTableError(table_name)
        return cols

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        if schema is None:
            schema = self.default_schema_name
        quote_schema = self.identifier_preparer.quote_identifier(schema)
        quote_view_name = self.identifier_preparer.quote_identifier(view_name)
        full_view_name = f"{quote_schema}.{quote_view_name}"

        # ToDo : perhaps can be removed if we get `SHOW CREATE VIEW`
        if view_name not in self.get_view_names(connection, schema):
            raise NoSuchTableError(full_view_name)

        query = f"""SHOW CREATE TABLE {full_view_name}"""
        try:
            view_def = connection.execute(text(query)).first()
            return view_def[1]
        except DBAPIError as e:
            if "1025" in e.orig.message:  # ToDo: The errors need parsing properly
                raise NoSuchTableError(full_view_name) from e

    def _get_column_type(self, column_type):
        pattern = r"(?:Nullable)*(?:\()*(\w+)(?:\((.*?)\))?(?:\))*"
        match = re.match(pattern, column_type)
        if match:
            type_str = match.group(1).lower()
            charlen = match.group(2)
            args = ()
            kwargs = {}
            if type_str == "decimal":
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
        table_name_query = """
            select table_name
            from information_schema.tables
            where table_schema = :schema_name
            and engine NOT LIKE '%VIEW%'
            """
        query = text(table_name_query).bindparams(
            bindparam("schema_name", type_=sqltypes.Unicode)
        )
        if schema is None:
            schema = self.default_schema_name

        result = connection.execute(query, dict(schema_name=schema))
        return [row[0] for row in result]

    @reflection.cache
    def get_temp_table_names(self, connection, schema=None, **kw):
        table_name_query = """
            select name
            from system.temporary_tables
            where database = :schema_name
            """
        query = text(table_name_query).bindparams(
            bindparam("schema_name", type_=sqltypes.Unicode)
        )
        if schema is None:
            schema = self.default_schema_name

        result = connection.execute(query, dict(schema_name=schema))
        return [row[0] for row in result]


    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        view_name_query = """
            select table_name
            from information_schema.tables
            where table_schema = :schema_name
            and engine LIKE '%VIEW%'
        """
        # This handles bug that existed a while, views were not included in information_schema.tables
        # https://github.com/databendlabs/databend/issues/16039
        if self.server_version_info > (1, 2, 410) and self.server_version_info <= (
            1,
            2,
            566,
        ):
            view_name_query = """
                select table_name
                from information_schema.views
                where table_schema = :schema_name
                """
        query = text(view_name_query).bindparams(
            bindparam("schema_name", type_=sqltypes.Unicode)
        )
        if schema is None:
            schema = self.default_schema_name

        result = connection.execute(query, dict(schema_name=schema))
        return [row[0] for row in result]

    @reflection.cache
    def get_table_options(self, connection, table_name, schema=None, **kw):
        options = {}

        # transient??
        # engine: str
        # cluster_by: list[expr]
        # engine_options: dict

        # engine_regex = r'ENGINE=(\w+)'
        # cluster_key_regex = r'CLUSTER BY \((.*)\)'
        query_text = """
            SELECT engine_full, cluster_by, is_transient
            FROM system.tables
            WHERE database = :schema_name
            and name = :table_name
            """
        # This handles bug that existed a while
        # https://github.com/databendlabs/databend/pull/16149
        if self.server_version_info > (1, 2, 410) and self.server_version_info <= (
            1,
            2,
            604,
        ):
            query_text = """
                SELECT engine_full, cluster_by, is_transient
                FROM system.tables
                WHERE database = :schema_name
                and name = :table_name

                UNION

                SELECT engine_full, NULL as cluster_by, NULL as is_transient
                FROM system.views
                WHERE database = :schema_name
                and name = :table_name
                """
        query = text(query_text).bindparams(
            bindparam("table_name", type_=sqltypes.Unicode),
            bindparam("schema_name", type_=sqltypes.Unicode),
        )
        if schema is None:
            schema = self.default_schema_name

        result = connection.execute(
            query, dict(table_name=table_name, schema_name=schema)
        ).one_or_none()
        if not result:
            raise NoSuchTableError(
                f"{self.identifier_preparer.quote_identifier(schema)}."
                f"{self.identifier_preparer.quote_identifier(table_name)}"
            )

        if result.engine_full:
            options["databend_engine"] = result.engine_full
        if result.cluster_by:
            cluster_by = re.match(r"\((.*)\)", result.cluster_by).group(1)
            options["databend_cluster_by"] = cluster_by
        if result.is_transient:
            options["databend_is_transient"] = result.is_transient

        # engine options

        return options

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema, **kw):
        query_text = """
            SELECT comment
            FROM system.tables
            WHERE database = :schema_name
            and name = :table_name
            """
        query = text(query_text).bindparams(
            bindparam("table_name", type_=sqltypes.Unicode),
            bindparam("schema_name", type_=sqltypes.Unicode),
        )
        if schema is None:
            schema = self.default_schema_name

        result = connection.execute(
            query, dict(table_name=table_name, schema_name=schema)
        ).one_or_none()
        if not result:
            raise NoSuchTableError(
                f"{self.identifier_preparer.quote_identifier(schema)}."
                f"{self.identifier_preparer.quote_identifier(table_name)}"
            )
        return {'text': result[0]} if result[0] else reflection.ReflectionDefaults.table_comment() if hasattr(reflection, 'ReflectionDefault') else {'text': None}

    def _prepare_filter_names(self, filter_names):
        if filter_names:
            fn = [name for name in filter_names]
            return True, {"filter_names": fn}
        else:
            return False, {}

    def get_multi_table_comment(
        self, connection, schema, filter_names, scope, kind, **kw
    ):
        meta = MetaData()
        all_tab_comments=Table(
            "tables",
            meta,
            Column("database", VARCHAR, nullable=False),
            Column("name", VARCHAR, nullable=False),
            Column("comment", VARCHAR),
            Column("table_type", VARCHAR),
            schema='system',
        ).alias("a_tab_comments")


        has_filter_names, params = self._prepare_filter_names(filter_names)
        owner = schema or self.default_schema_name

        table_types = set()
        if reflection.ObjectKind.TABLE in kind:
            table_types.add('BASE TABLE')
        if reflection.ObjectKind.VIEW in kind:
            table_types.add('VIEW')

        query = select(
            all_tab_comments.c.name, all_tab_comments.c.comment
        ).where(
            all_tab_comments.c.database == owner,
            all_tab_comments.c.table_type.in_(table_types),
            sqlalchemy.true() if reflection.ObjectScope.DEFAULT in scope else sqlalchemy.false(),
        )
        if has_filter_names:
            query = query.where(all_tab_comments.c.name.in_(bindparam("filter_names")))

        result = connection.execute(query, params)
        default_comment = reflection.ReflectionDefaults.table_comment
        return (
            (
                (schema, table),
                {"text": comment} if comment else default_comment(),
            )
            for table, comment in result
        )

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
    pattern = r"Nullable\((\w+)(?:\((.*?)\))?\)"
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
