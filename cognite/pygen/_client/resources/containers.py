from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from cognite.pygen._client.models import Container, ContainerReference, ContainerRequest, ContainerResponse

from .resources import ResourceAPI


class ContainersAPI(ResourceAPI[ContainerRequest, ContainerResponse, ContainerReference]):
    """Containers resource client."""

    resource_path = "/models/containers"
    request_cls = ContainerRequest
    response_cls = ContainerResponse

    def _to_reference(self, value: Any) -> ContainerReference:
        if isinstance(value, ContainerReference):
            return value
        if isinstance(value, ContainerResponse | Container):
            return value.as_reference()
        if isinstance(value, tuple) and len(value) == 2:
            space, external_id = value
            if not all(isinstance(item, str) for item in (space, external_id)):
                raise TypeError("Tuple reference for Container must contain two strings")
            return ContainerReference(space=space, external_id=external_id)
        raise TypeError("Unsupported reference type for Container")

    def _to_request(self, value: Any) -> ContainerRequest:
        if isinstance(value, ContainerRequest):
            return value
        if isinstance(value, ContainerResponse):
            return value.as_request()
        if isinstance(value, Container):
            return ContainerRequest.model_validate(value.model_dump(by_alias=True))
        raise TypeError("Unsupported resource type for Container")

    def iterate(self, *, space: str | None = None, limit: int | None = None) -> Iterator[ContainerResponse]:
        return super().iterate(space=space, limit=limit)

    def list(self, *, space: str | None = None, limit: int | None = None) -> list[ContainerResponse]:
        return list(super().iterate(space=space, limit=limit))
