"""Views API resource client.

This module provides the ViewsAPI class for managing CDF views.
"""

from collections.abc import Sequence

from cognite.pygen._client.models import ViewReference, ViewRequest, ViewResponse
from cognite.pygen._generation.python.instance_api.http_client import HTTPClient, SuccessResponse

from ._base import BaseResourceAPI, Page, ReferenceResponseItems, ResourceLimits


class ViewsAPI(BaseResourceAPI[ViewReference, ViewRequest, ViewResponse]):
    """API client for CDF View resources.

    Views define the structure and properties that can be queried on nodes and edges.
    They provide a schema layer on top of containers.

    """

    def __init__(self, http_client: HTTPClient, limits: ResourceLimits | None = None) -> None:
        """Initialize the Views API.

        Args:
            http_client: The HTTP client to use for API requests.
            limits: Configuration for API endpoint limits. Uses defaults if not provided.
        """
        super().__init__(http_client, "/models/views", limits)

    def _page_response(self, response: SuccessResponse) -> Page[ViewResponse]:
        return Page[ViewResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ReferenceResponseItems[ViewReference]:
        return ReferenceResponseItems[ViewReference].model_validate_json(response.body)

    def create(self, items: Sequence[ViewRequest]) -> list[ViewResponse]:
        """Create or update views.

        Args:
            items: A sequence of request objects defining the views to create/update.

        Returns:
            A list of the created/updated view objects.
        """
        return self._create(items)

    def retrieve(
        self,
        references: Sequence[ViewReference],
        include_inherited_properties: bool = True,
    ) -> list[ViewResponse]:
        """Retrieve specific views by their references.

        Args:
            references: A sequence of reference objects identifying the views to retrieve.
            include_inherited_properties: Whether to include inherited properties (default: True)

        Returns:
            A list of view objects. Views that don't exist are not included.
        """
        return self._retrieve(
            references,
            params={"includeInheritedProperties": include_inherited_properties},
        )

    def delete(self, references: Sequence[ViewReference]) -> list[ViewReference]:
        """Delete views by their references.

        Args:
            references: A sequence of reference objects identifying the views to delete.

        Returns:
            A list of references to the deleted views.
        """
        return self._delete(references)

    def iterate(
        self,
        space: str | None = None,
        include_global: bool = False,
        all_versions: bool = False,
        include_inherited_properties: bool = True,
        limit: int = 10,
        cursor: str | None = None,
    ) -> Page[ViewResponse]:
        """Fetch a single page of views.

        Args:
            space: Filter by space (default: None, meaning all spaces)
            include_global: Whether to include global views (default: False)
            all_versions: Whether to return all versions (default: False)
            include_inherited_properties: Whether to include inherited properties (default: True)
            limit: Maximum number of items per page (default: 10)
            cursor: Cursor for pagination (default: None)

        Returns:
            A Page containing the views and the cursor for the next page.
        """
        return self._iterate(
            limit,
            cursor,
            {
                "space": space,
                "includeGlobal": include_global,
                "allVersions": all_versions,
                "includeInheritedProperties": include_inherited_properties,
            },
        )

    def list(
        self,
        space: str | None = None,
        include_global: bool = False,
        all_versions: bool = False,
        include_inherited_properties: bool = True,
        limit: int | None = 10,
    ) -> list[ViewResponse]:
        """List all views, handling pagination automatically.

        This method fetches all views, handling pagination transparently.

        Args:
            space: Filter by space (default: None, meaning all spaces)
            include_global: Whether to include global views (default: False)
            all_versions: Whether to return all versions (default: False)
            include_inherited_properties: Whether to include inherited properties (default: True)
            limit: Maximum number of items to retrieve (default: None, meaning no limit)

        Returns:
            A list of all view objects.
        """
        return self._list(
            limit,
            {
                "space": space,
                "includeGlobal": include_global,
                "allVersions": all_versions,
                "includeInheritedProperties": include_inherited_properties,
            },
        )
