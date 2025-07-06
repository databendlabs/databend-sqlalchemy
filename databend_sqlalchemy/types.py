from __future__ import annotations

import datetime as dt
from typing import Optional, Type, Any

from sqlalchemy import func
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql import type_api


# ToDo - This is perhaps how numeric should be defined
# class NUMERIC(sqltypes.Numeric):
#     def result_processor(self, dialect, type_):
#
#         orig = super().result_processor(dialect, type_)
#
#         def process(value):
#             if value is not None:
#                 if self.decimal_return_scale:
#                     value = decimal.Decimal(f'{value:.{self.decimal_return_scale}f}')
#                 else:
#                     value = decimal.Decimal(value)
#             if orig:
#                 return orig(value)
#             return value
#
#         return process


class INTERVAL(type_api.NativeForEmulated, sqltypes._AbstractInterval):
    """Databend INTERVAL type."""

    __visit_name__ = "INTERVAL"
    native = True

    def __init__(
        self, precision: Optional[int] = None, fields: Optional[str] = None
    ) -> None:
        """Construct an INTERVAL.

        :param precision: optional integer precision value
        :param fields: string fields specifier.  allows storage of fields
         to be limited, such as ``"YEAR"``, ``"MONTH"``, ``"DAY TO HOUR"``,
         etc.

        """
        self.precision = precision
        self.fields = fields

    @classmethod
    def adapt_emulated_to_native(
        cls,
        interval: sqltypes.Interval,
        **kw: Any,  # type: ignore[override]
    ) -> INTERVAL:
        return INTERVAL(precision=interval.second_precision)

    @property
    def _type_affinity(self) -> Type[sqltypes.Interval]:
        return sqltypes.Interval

    def as_generic(self, allow_nulltype: bool = False) -> sqltypes.Interval:
        return sqltypes.Interval(native=True, second_precision=self.precision)

    @property
    def python_type(self) -> Type[dt.timedelta]:
        return dt.timedelta

    def literal_processor(
        self, dialect: Dialect
    ) -> Optional[type_api._LiteralProcessorType[dt.timedelta]]:
        def process(value: dt.timedelta) -> str:
            return f"to_interval('{value.total_seconds()} seconds')"

        return process


class TINYINT(sqltypes.Integer):
    __visit_name__ = "TINYINT"
    native = True


class DOUBLE(sqltypes.Float):
    __visit_name__ = "DOUBLE"
    native = True


class FLOAT(sqltypes.Float):
    __visit_name__ = "FLOAT"
    native = True


#  The “CamelCase” types are to the greatest degree possible database agnostic

#  For these datatypes, specific SQLAlchemy dialects provide backend-specific “UPPERCASE” datatypes, for a SQL type that has no analogue on other backends


class BITMAP(sqltypes.TypeEngine):
    __visit_name__ = "BITMAP"
    render_bind_cast = True

    def __init__(self, **kwargs):
        super(BITMAP, self).__init__()

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        # Databend returns bitmaps as strings of comma-separated integers
        return set(int(x) for x in value.split(',') if x)

    def bind_expression(self, bindvalue):
        return func.to_bitmap(bindvalue, type_=self)

    def column_expression(self, col):
        # Convert bitmap to string using a custom function
        return func.to_string(col, type_=sqltypes.String)

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            if isinstance(value, set):
                return ','.join(str(x) for x in sorted(value))
            return str(value)
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            return set(int(x) for x in value.split(',') if x)
        return process


class GEOMETRY(sqltypes.TypeEngine):
    __visit_name__ = "GEOMETRY"

    def __init__(self, srid=None):
        super(GEOMETRY, self).__init__()
        self.srid = srid



class GEOGRAPHY(sqltypes.TypeEngine):
    __visit_name__ = "GEOGRAPHY"
    native = True

    def __init__(self, srid=None):
        super(GEOGRAPHY, self).__init__()
        self.srid = srid


