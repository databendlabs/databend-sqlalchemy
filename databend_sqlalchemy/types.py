from __future__ import annotations

import datetime as dt
from typing import Optional, Type, Any

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
