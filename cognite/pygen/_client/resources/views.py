from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from cognite.pygen._client.models import View, ViewReference, ViewRequest, ViewResponse

from .resources import ResourceAPI


class ViewsAPI(ResourceAPI[ViewRequest, ViewResponse, ViewReference]):
    """Views resource client."""

    resource_path = "/models/views"
    request_cls = ViewRequest
    response_cls = ViewResponse

    def _to_reference(self, value: Any) -> ViewReference:
        if isinstance(value, ViewReference):
            return value
        if isinstance(value, ViewResponse | View):
            return value.as_reference()
        if isinstance(value, tuple) and len(value) == 3:
            space, external_id, version = value
            if not all(isinstance(item, str) for item in (space, external_id, version)):
                raise TypeError("Tuple reference for View must contain three strings")
            return ViewReference(space=space, external_id=external_id, version=version)
        raise TypeError("Unsupported reference type for View")

    def _to_request(self, value: Any) -> ViewRequest:
        if isinstance(value, ViewRequest):
            return value
        if isinstance(value, ViewResponse):
            return value.as_request()
        if isinstance(value, View):
            return ViewRequest.model_validate(value.model_dump(by_alias=True))
        raise TypeError("Unsupported resource type for View")

    def iterate(self, *, space: str | None = None, limit: int | None = None) -> Iterator[ViewResponse]:
        return super().iterate(space=space, limit=limit)

    def list(self, *, space: str | None = None, limit: int | None = None) -> list[ViewResponse]:
        return list(super().iterate(space=space, limit=limit))
