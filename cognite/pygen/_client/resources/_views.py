"""Views API resource client.

This module provides the ViewsAPI class for managing CDF views.
"""

from __future__ import annotations

from collections.abc import Iterator

from pydantic import TypeAdapter

from cognite.pygen._client.http_client import HTTPClient, RequestMessage
from cognite.pygen._client.models import ViewReference, ViewRequest, ViewResponse

from ._base import BaseResourceAPI, Page


class ViewsAPI(BaseResourceAPI[ViewReference, ViewRequest, ViewResponse]):
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
        super().__init__(http_client)
        self._response_type_adapter = TypeAdapter(list[ViewResponse])
        self._reference_type_adapter = TypeAdapter(list[ViewReference])
        self._request_type_adapter = TypeAdapter(list[ViewRequest])

    @property
    def _endpoint(self) -> str:
        return "/models/views"

    @property
    def _response_adapter(self) -> TypeAdapter[list[ViewResponse]]:
        return self._response_type_adapter

    @property
    def _reference_adapter(self) -> TypeAdapter[list[ViewReference]]:
        return self._reference_type_adapter

    @property
    def _request_adapter(self) -> TypeAdapter[list[ViewRequest]]:
        return self._request_type_adapter

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
        params: dict[str, str | int | bool] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        if space is not None:
            params["space"] = space
        if include_global:
            params["includeGlobal"] = True
        if all_versions:
            params["allVersions"] = True
        if not include_inherited_properties:
            params["includeInheritedProperties"] = False

        request = RequestMessage(
            endpoint_url=self._make_url(),
            method="GET",
            parameters=params,
        )

        result = self._http_client.request_with_retries(request)
        response = result.get_success_or_raise()

        return self._parse_list_response(response)

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
        cursor: str | None = None
        count = 0
        page_limit = min(limit, 1000) if limit is not None else 1000

        while True:
            page = self.iterate(
                space=space,
                cursor=cursor,
                limit=page_limit,
                include_global=include_global,
                all_versions=all_versions,
                include_inherited_properties=include_inherited_properties,
            )

            for item in page.items:
                yield item
                count += 1
                if limit is not None and count >= limit:
                    return

            if page.cursor is None:
                break
            cursor = page.cursor
