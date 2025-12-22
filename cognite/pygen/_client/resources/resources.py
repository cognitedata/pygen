from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Generic, TypeVar

from pydantic.alias_generators import to_camel

from cognite.pygen._client.config import PygenClientConfig
from cognite.pygen._client.http_client import HTTPClient, RequestMessage
from cognite.pygen._client.models import APIResource, ReferenceObject, ResponseResource

ReferenceT = TypeVar("ReferenceT", bound=ReferenceObject)
RequestT = TypeVar("RequestT", bound=APIResource[ReferenceT])
ResponseT = TypeVar("ResponseT", bound=ResponseResource[ReferenceT, RequestT])


class ResourceAPI(Generic[RequestT, ResponseT, ReferenceT], ABC):
    """Common functionality for Data Modeling resource APIs."""

    resource_path: str

    def __init__(self, http_client: HTTPClient, config: PygenClientConfig) -> None:
        if not getattr(self, "resource_path", None):
            raise ValueError("ResourceAPI subclasses must define resource_path")
        if not self.resource_path.startswith("/"):
            raise ValueError("resource_path must start with '/'")
        self._http_client = http_client
        self._config = config

    @property
    @abstractmethod
    def request_cls(self) -> type[RequestT]:
        """Pydantic model used for create requests."""

    @property
    @abstractmethod
    def response_cls(self) -> type[ResponseT]:
        """Pydantic model used for responses."""

    @abstractmethod
    def _to_reference(self, value: Any) -> ReferenceT:
        """Convert user input to a reference model."""

    @abstractmethod
    def _to_request(self, value: Any) -> RequestT:
        """Convert user input to a request model."""

    def iterate(self, *, limit: int | None = None, **params: Any) -> Iterator[ResponseT]:
        """Yield items lazily, handling pagination."""
        remaining = limit
        cursor: str | None = None
        while True:
            page_limit = remaining if remaining is not None else None
            page_params = {"cursor": cursor, "limit": page_limit, **params}
            payload = self._list(page_params)
            items = payload.get("items", [])
            for item in items:
                yield self.response_cls.model_validate(item)
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return
            cursor = payload.get("nextCursor") or payload.get("cursor")
            if not cursor:
                break

    def list(self, *, limit: int | None = None, **params: Any) -> list[ResponseT]:
        """Return a materialized list of items."""
        return list(self.iterate(limit=limit, **params))

    def retrieve(self, identifiers: Any) -> ResponseT | list[ResponseT] | None:
        """Retrieve one or more resources by reference."""
        refs = self._ensure_sequence(identifiers)
        body = {"items": [self._to_reference(ref).model_dump(by_alias=True) for ref in refs]}
        payload = self._post_json(f"{self.resource_path}/byids", body)
        responses = [self.response_cls.model_validate(item) for item in payload.get("items", [])]
        if self._is_single_input(identifiers):
            return responses[0] if responses else None
        return responses

    def create(self, resources: Any) -> ResponseT | list[ResponseT]:
        """Create one or more resources."""
        requests = [self._to_request(item) for item in self._ensure_sequence(resources)]
        body = {"items": [resource.model_dump(by_alias=True, exclude_none=True) for resource in requests]}
        payload = self._post_json(self.resource_path, body)
        responses = [self.response_cls.model_validate(item) for item in payload.get("items", [])]
        return responses[0] if self._is_single_input(resources) else responses

    def delete(self, identifiers: Any) -> None:
        """Delete one or more resources by reference."""
        refs = [self._to_reference(ref) for ref in self._ensure_sequence(identifiers)]
        body = {"items": [ref.model_dump(by_alias=True) for ref in refs]}
        self._post_json(f"{self.resource_path}/delete", body)

    def _list(self, params: Mapping[str, Any] | None) -> dict[str, Any]:
        message = RequestMessage(
            endpoint_url=self._config.create_api_url(self.resource_path),
            method="GET",
            parameters=self._clean_params(params),
        )
        response = self._http_client.request_with_retries(message).get_success_or_raise()
        return response.body_json

    def _post_json(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        message = RequestMessage(
            endpoint_url=self._config.create_api_url(path),
            method="POST",
            body_content=body,
        )
        response = self._http_client.request_with_retries(message).get_success_or_raise()
        return response.body_json

    @staticmethod
    def _clean_params(params: Mapping[str, Any] | None) -> dict[str, str | int | float | bool] | None:
        if params is None:
            return None
        cleaned: dict[str, str | int | float | bool] = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, (str, int, float, bool)):
                cleaned[to_camel(key)] = value
            else:
                cleaned[to_camel(key)] = str(value)
        return cleaned or None

    @staticmethod
    def _ensure_sequence(value: Any) -> Sequence[Any]:
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return value
        return [value]

    @staticmethod
    def _is_single_input(value: Any) -> bool:
        return not (isinstance(value, Sequence) and not isinstance(value, (str, bytes)))
