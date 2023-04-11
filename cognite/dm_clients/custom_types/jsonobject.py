from __future__ import annotations

import json
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


__all__ = [
    "JSONObject",
]


class JSONObject(dict):
    """
    Python dict that can be serialized into JSON format.
    Note: DM supports lists as well, but this implementation does not!
    """

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema["description"] = "JSON-serializable Python dict."

    @classmethod
    def validate(cls, value: Dict[str, Any]) -> JSONObject:
        json.dumps(value)
        return cls(value)

    def __repr__(self) -> str:
        return f"JSONObject({super().__repr__()})"
