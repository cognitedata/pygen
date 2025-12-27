from datetime import date, datetime
from typing import Annotated

from pydantic import BeforeValidator, PlainSerializer

from cognite.pygen._generation.python.instance_api._utils import datetime_to_ms, ms_to_datetime

DateTimeMS = Annotated[
    datetime,
    BeforeValidator(ms_to_datetime, json_schema_input_type=int),
    PlainSerializer(datetime_to_ms, return_type=int, when_used="always"),
]
DateTime = Annotated[
    datetime, PlainSerializer(lambda d: d.isoformat(timespec="milliseconds"), return_type=str, when_used="always")
]
Date = Annotated[date, PlainSerializer(lambda d: d.isoformat(), return_type=str, when_used="always")]
