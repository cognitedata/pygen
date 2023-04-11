from __future__ import annotations

import logging
import re
from contextlib import suppress
from datetime import datetime
from math import floor
from typing import TypeVar

logger = logging.getLogger(__name__)


__all__ = [
    "Timestamp",
]


TimestampParsable = TypeVar("TimestampParsable", str, datetime)


class Timestamp(str):
    """
    String in the format 'YYYY-MM-DDTHH:MM:SS[.millis][Z|time zone]' with optional milliseconds having precision
    of 1-3 decimal digits and optional timezone with format ±HH:MM, ±HHMM, ±HH or Z, where Z represents UTC, Year
    must be between 0001 and 9999.

    Value must conform to this format to be accepted.
    One exception to this is that additional decimal digits of the second will be accepted but the value will be cut
    (floored) to 3 decimal digits, e.g. 2023-04-05T11:12:13.4567 will be converted to 2023-04-05T11:12:13.456.
    We do flooring rather than rounding to avoid moving the value into the future.

    This class is a custom Pydantic type:

    >>> from pydantic import BaseModel
    >>> class Foo(BaseModel):
    ...     bar: Timestamp

    # timezones:
    >>> Foo(bar="2023-04-05T06:07:08")
    Foo(bar=Timestamp('2023-04-05T06:07:08'))
    >>> Foo(bar="2023-04-05T06:07:08+09:00")
    Foo(bar=Timestamp('2023-04-05T06:07:08+09:00'))
    >>> Foo(bar="2023-04-05T06:07:08+0900")
    Foo(bar=Timestamp('2023-04-05T06:07:08+0900'))
    >>> Foo(bar="2023-04-05T06:07:08+09")
    Foo(bar=Timestamp('2023-04-05T06:07:08+09'))
    >>> Foo(bar="2023-04-05T06:07:08Z")
    Foo(bar=Timestamp('2023-04-05T06:07:08Z'))

    # milliseconds:
    >>> Foo(bar="2023-04-05T06:07:08.123456")
    Foo(bar=Timestamp('2023-04-05T06:07:08.123'))
    >>> Foo(bar="2023-04-05T06:07:08.1256Z")
    Foo(bar=Timestamp('2023-04-05T06:07:08.125Z'))
    >>> Foo(bar="2023-04-05T06:07:08.12Z")
    Foo(bar=Timestamp('2023-04-05T06:07:08.12Z'))

    # edge years:
    >>> Foo(bar="9999-04-05T06:07:08.12Z")
    Foo(bar=Timestamp('9999-04-05T06:07:08.12Z'))
    >>> Foo(bar="0001-04-05T06:07:08.12Z")
    Foo(bar=Timestamp('0001-04-05T06:07:08.12Z'))

    # pass in datetime objects:
    >>> from datetime import datetime, timedelta, timezone
    >>> Foo(bar=datetime(2023, 4, 5, 6, 7, 8))
    Foo(bar=Timestamp('2023-04-05T06:07:08'))
    >>> Foo(bar=datetime(2023, 4, 5, 6, 7, 8, 123456))
    Foo(bar=Timestamp('2023-04-05T06:07:08.123'))
    >>> Foo(bar=datetime(2023, 4, 5, 6, 7, 8, 345678))
    Foo(bar=Timestamp('2023-04-05T06:07:08.345'))
    >>> Foo(bar=datetime(2023, 4, 5, 6, 7, 8, 345678, timezone(timedelta(hours=-5), 'EST')))
    Foo(bar=Timestamp('2023-04-05T06:07:08.345-05:00'))
    """

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema["description"] = (
            "String in the format 'YYYY-MM-DDTHH:MM:SS[.millis][Z|time zone]' with optional milliseconds having"
            " precision of 1-3 decimal digits and optional timezone with format ±HH:MM, ±HHMM, ±HH or Z, where Z"
            " represents UTC, Year must be between 0001 and 9999."
        )

    @classmethod
    def _parse_str(cls, value: str) -> datetime:
        """Parse the value, returning a datetime and the format string."""

        # Add minutes to the timezone offset if missing
        if re.match(r".*[+-]\d\d$", value):
            value = f"{value}00"

        formats = (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        )

        for fmt in formats:
            with suppress(ValueError):
                return datetime.strptime(value, fmt)
        raise ValueError(f"Cannot parse Timestamp: {value}")

    @classmethod
    def validate(cls, value: TimestampParsable) -> Timestamp:
        str_value: str
        datetime_value: datetime

        if isinstance(value, str):
            datetime_value = cls._parse_str(value)
            str_value = value

        else:
            datetime_value = value  # assuming datetime
            str_value = value.isoformat()

        # Milliseconds need some special treatment:
        milliseconds = floor(datetime_value.microsecond / 1000)
        try:
            microseconds_str = re.findall(r".{19}\.(\d+).*", str_value)[0]
        except IndexError:
            microseconds_str = ""
        if len(microseconds_str) > 3:
            str_value = re.sub(r"(.{19})\.(\d+)(.*)", rf"\1.{milliseconds:03}\3", str_value)

        return cls(str_value)

    def __repr__(self) -> str:
        return f"Timestamp({super().__repr__()})"

    def datetime(self) -> datetime:
        return Timestamp._parse_str(self)
