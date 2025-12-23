import gzip
from abc import ABC, abstractmethod
from typing import Any, Literal

import httpx
from pydantic import BaseModel, JsonValue, TypeAdapter, model_validator

from cognite.pygen._generation.python.instance_api.exceptions import PygenAPIError


class HTTPResult(BaseModel):
    def get_success_or_raise(self) -> "SuccessResponse":
        """Raises an exception if any response in the list indicates a failure."""
        if isinstance(self, SuccessResponse):
            return self
        elif isinstance(self, FailedResponse):
            raise PygenAPIError(f"Request failed with status code {self.status_code}: {self.error.message}")
        elif isinstance(self, FailedRequest):
            raise PygenAPIError(f"Request failed with error: {self.error}")
        else:
            raise PygenAPIError("Unknown HTTPResult type")


class FailedRequest(HTTPResult):
    error: str


class SuccessResponse(HTTPResult):
    status_code: int
    body: str
    content: bytes


class ErrorDetails(BaseModel):
    """This is the expected structure of error details in the CDF API"""

    code: int
    message: str
    missing: list[JsonValue] | None = None
    duplicated: list[JsonValue] | None = None
    is_auto_retryable: bool | None = None

    @classmethod
    def from_response(cls, response: httpx.Response) -> "ErrorDetails":
        """Populate the error details from a httpx response."""
        try:
            res = TypeAdapter(dict[Literal["error"], ErrorDetails]).validate_json(response.text)
        except ValueError:
            return cls(code=response.status_code, message=response.text)
        return res["error"]


class FailedResponse(HTTPResult):
    status_code: int
    body: str
    error: ErrorDetails


class BaseRequestMessage(BaseModel, ABC):
    endpoint_url: str
    method: Literal["GET", "POST", "PATCH", "DELETE", "PUT"]
    connect_attempt: int = 0
    read_attempt: int = 0
    status_attempt: int = 0
    api_version: str | None = None
    content_type: str = "application/json"
    accept: str = "application/json"
    disable_gzip: bool = False

    parameters: dict[str, str | int | float | bool] | None = None

    @property
    def total_attempts(self) -> int:
        return self.connect_attempt + self.read_attempt + self.status_attempt

    @property
    @abstractmethod
    def content(self) -> str | bytes | None: ...


class RequestMessage(BaseRequestMessage):
    data_content: bytes | None = None
    body_content: dict[str, JsonValue] | None = None

    @model_validator(mode="before")
    def check_data_or_body(cls, values: dict[str, Any]) -> dict[str, Any]:
        if values.get("data_content") is not None and values.get("body_content") is not None:
            raise ValueError("Only one of data_content or body_content can be set.")
        return values

    @property
    def content(self) -> str | bytes | None:
        data: str | bytes | None = None
        if self.data_content is not None:
            data = self.data_content
            if not self.disable_gzip:
                data = gzip.compress(data)
        elif self.body_content is not None:
            # We serialize using pydantic instead of json.dumps. This is because pydantic is faster
            # and handles more complex types such as datetime, float('nan'), etc.
            data = _BODY_SERIALIZER.dump_json(self.body_content)
            if not self.disable_gzip and isinstance(data, bytes):
                data = gzip.compress(data)
        return data


_BODY_SERIALIZER = TypeAdapter(dict[str, JsonValue])
