
from __future__ import annotations

import datetime as dt
import re
from typing import Optional, Type, Any

from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql import type_api

INTERVAL_RE = re.compile(
    r"^"
    r"(?:(?P<days>-?\d+) (days? ?))?"
    r"(?:(?P<sign>[-+])?"
    r"(?P<hours>\d+):"
    r"(?P<minutes>\d\d):"
    r"(?P<seconds>\d\d)"
    r"(?:\.(?P<microseconds>\d{1,6}))?"
    r")?$"
)


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
        cls, interval: sqltypes.Interval, **kw: Any  # type: ignore[override]
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

    # ToDo - If BendSQL returns a timedelta for interval, then we should be able to remove this method
    def result_processor(
        self, dialect: Dialect, coltype: Any
    ) -> type_api._ResultProcessorType[dt.timedelta]:

        def process(value: Any) -> Optional[dt.timedelta]:
            """Parse a duration string and return a datetime.timedelta."""
            if value is None:
                return None

            match = INTERVAL_RE.match(value)
            if match:
                kw = match.groupdict()
                sign = -1 if kw.pop("sign", "+") == "-" else 1
                if kw.get("microseconds"):
                    kw["microseconds"] = kw["microseconds"].ljust(6, "0")
                kw = {k: float(v.replace(",", ".")) for k, v in kw.items() if v is not None}
                days = dt.timedelta(kw.pop("days", 0.0) or 0.0)

                return days + sign * dt.timedelta(**kw)

        return process
