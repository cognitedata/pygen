from strawberry import scalar

from .jsonobject import JSONObject
from .timestamp import Timestamp

__all__ = [
    "JSONObjectScalar",
    "TimestampScalar",
]

JSONObjectScalar = scalar(JSONObject)
TimestampScalar = scalar(Timestamp)
