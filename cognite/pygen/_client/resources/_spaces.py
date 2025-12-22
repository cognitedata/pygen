"""Spaces API resource client.

This module provides the SpacesAPI class for managing CDF spaces.
"""

from __future__ import annotations

from pydantic import TypeAdapter

from cognite.pygen._client.http_client import HTTPClient
from cognite.pygen._client.models import SpaceReference, SpaceRequest, SpaceResponse

from ._base import BaseResourceAPI


class SpacesAPI(BaseResourceAPI[SpaceReference, SpaceRequest, SpaceResponse]):
    """API client for CDF Space resources.

    Spaces are containers that organize data modeling resources like
    data models, views, and containers.

    Example:
        >>> from cognite.pygen._client import PygenClient
        >>> client = PygenClient(config)
        >>> # List all spaces
        >>> for space in client.spaces.list():
        ...     print(space.space)
        >>> # Retrieve specific spaces
        >>> spaces = client.spaces.retrieve([SpaceReference(space="my_space")])
        >>> # Create a space
        >>> created = client.spaces.create([SpaceRequest(space="new_space", name="New Space")])
        >>> # Delete a space
        >>> deleted = client.spaces.delete([SpaceReference(space="my_space")])
    """

    def __init__(self, http_client: HTTPClient) -> None:
        """Initialize the Spaces API.

        Args:
            http_client: The HTTP client to use for API requests.
        """
        super().__init__(http_client)
        self._response_type_adapter = TypeAdapter(list[SpaceResponse])
        self._reference_type_adapter = TypeAdapter(list[SpaceReference])
        self._request_type_adapter = TypeAdapter(list[SpaceRequest])

    @property
    def _endpoint(self) -> str:
        return "/models/spaces"

    @property
    def _response_adapter(self) -> TypeAdapter[list[SpaceResponse]]:
        return self._response_type_adapter

    @property
    def _reference_adapter(self) -> TypeAdapter[list[SpaceReference]]:
        return self._reference_type_adapter

    @property
    def _request_adapter(self) -> TypeAdapter[list[SpaceRequest]]:
        return self._request_type_adapter
