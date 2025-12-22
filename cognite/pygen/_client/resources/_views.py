"""Views API resource client.

This module provides the ViewsAPI class for managing CDF views.
"""

from __future__ import annotations

import builtins
from collections.abc import Iterator, Sequence

from cognite.pygen._client.http_client import HTTPClient
from cognite.pygen._client.models import ViewReference, ViewRequest, ViewResponse

from ._base import BaseResourceAPI, Page


class ViewsAPI:
    """API client for CDF View resources.

    Views define the structure and properties that can be queried on nodes and edges.
    They provide a schema layer on top of containers.

    Example:
        >>> from cognite.pygen._client import PygenClient
        >>> client = PygenClient(config)
        >>> # List all views
        >>> for view in client.views.list():
        ...     print(f"{view.space}:{view.external_id}")
        >>> # Retrieve specific views
        >>> ref = ViewReference(space="my_space", external_id="my_view", version="v1")
        >>> views = client.views.retrieve([ref])
    """

    def __init__(self, http_client: HTTPClient) -> None:
        """Initialize the Views API.

        Args:
            http_client: The HTTP client to use for API requests.
        """
        self._api = BaseResourceAPI[ViewReference, ViewResponse](
            http_client=http_client,
            endpoint="/models/views",
            reference_cls=ViewReference,
            request_cls=ViewRequest,
            response_cls=ViewResponse,
        )

    def iterate(
        self,
        *,
        space: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
        include_global: bool = False,
        all_versions: bool = False,
        include_inherited_properties: bool = True,
    ) -> Page[ViewResponse]:
        """Fetch a single page of views.

        Args:
            space: Filter by space. If None, returns views from all spaces.
            cursor: Cursor for pagination. If None, starts from the beginning.
            limit: Maximum number of items to return per page. Default is 100.
            include_global: Whether to include global views. Default is False.
            all_versions: Whether to return all versions of each view. Default is False.
            include_inherited_properties: Whether to include inherited properties. Default is True.

        Returns:
            A Page containing the views and the cursor for the next page.
        """
        extra_params: dict[str, str | int | bool] = {}
        if all_versions:
            extra_params["allVersions"] = True
        if not include_inherited_properties:
            extra_params["includeInheritedProperties"] = False

        return self._api.iterate(
            space=space,
            cursor=cursor,
            limit=limit,
            include_global=include_global,
            extra_params=extra_params if extra_params else None,
        )

    def list(
        self,
        *,
        space: str | None = None,
        include_global: bool = False,
        limit: int | None = None,
        all_versions: bool = False,
        include_inherited_properties: bool = True,
    ) -> Iterator[ViewResponse]:
        """List all views, handling pagination automatically.

        This method lazily iterates over all views, fetching pages as needed.

        Args:
            space: Filter by space. If None, returns views from all spaces.
            include_global: Whether to include global views. Default is False.
            limit: Maximum total number of items to return. If None, returns all items.
            all_versions: Whether to return all versions of each view. Default is False.
            include_inherited_properties: Whether to include inherited properties. Default is True.

        Yields:
            ViewResponse objects from the API.
        """
        extra_params: dict[str, str | int | bool] = {}
        if all_versions:
            extra_params["allVersions"] = True
        if not include_inherited_properties:
            extra_params["includeInheritedProperties"] = False

        return self._api.list(
            space=space,
            include_global=include_global,
            limit=limit,
            extra_params=extra_params if extra_params else None,
        )

    def retrieve(self, references: Sequence[ViewReference]) -> builtins.list[ViewResponse]:
        """Retrieve specific views by their references.

        Args:
            references: A sequence of reference objects identifying the views to retrieve.

        Returns:
            A list of view objects. Views that don't exist are not included.
        """
        return self._api.retrieve(references)

    def create(self, items: Sequence[ViewRequest]) -> builtins.list[ViewResponse]:
        """Create or update views.

        Args:
            items: A sequence of request objects defining the views to create/update.

        Returns:
            A list of the created/updated view objects.
        """
        return self._api.create(items)

    def delete(self, references: Sequence[ViewReference]) -> builtins.list[ViewReference]:
        """Delete views by their references.

        Args:
            references: A sequence of reference objects identifying the views to delete.

        Returns:
            A list of references to the deleted views.
        """
        return self._api.delete(references)
