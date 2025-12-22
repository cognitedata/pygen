from __future__ import annotations

from typing import Any

from cognite.pygen._client.models import Space, SpaceReference, SpaceRequest, SpaceResponse

from .resources import ResourceAPI


class SpacesAPI(ResourceAPI[SpaceRequest, SpaceResponse, SpaceReference]):
    """Spaces resource client."""

    resource_path = "/models/spaces"
    request_cls = SpaceRequest
    response_cls = SpaceResponse

    def _to_reference(self, value: Any) -> SpaceReference:
        if isinstance(value, SpaceReference):
            return value
        if isinstance(value, SpaceResponse | Space):
            return value.as_reference()
        if isinstance(value, str):
            return SpaceReference(space=value)
        raise TypeError("Unsupported reference type for Space")

    def _to_request(self, value: Any) -> SpaceRequest:
        if isinstance(value, SpaceRequest):
            return value
        if isinstance(value, SpaceResponse):
            return value.as_request()
        if isinstance(value, Space):
            return SpaceRequest.model_validate(value.model_dump(by_alias=True))
        raise TypeError("Unsupported resource type for Space")
