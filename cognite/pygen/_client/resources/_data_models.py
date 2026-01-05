"""Data Models API resource client.

This module provides the DataModelsAPI class for managing CDF data models.
"""

from collections.abc import Sequence
from typing import Literal, overload

from cognite.pygen._client.models import (
    DataModelReference,
    DataModelRequest,
    DataModelResponse,
    DataModelResponseWithViews,
)
from cognite.pygen._python.instance_api.http_client import HTTPClient, RequestMessage, SuccessResponse
from cognite.pygen._utils.collection import chunker_sequence

from ._base import BaseResourceAPI, Page, ReferenceResponseItems, ResourceLimits


class DataModelsAPI(BaseResourceAPI[DataModelReference, DataModelRequest, DataModelResponse]):
    """API client for CDF Data Model resources.

    Data models group and structure views into reusable collections.
    A data model contains a set of views where the node types can
    refer to each other with direct relations and edges.

    """

    def __init__(self, http_client: HTTPClient, limits: ResourceLimits | None = None) -> None:
        """Initialize the Data Models API.

        Args:
            http_client: The HTTP client to use for API requests.
            limits: Configuration for API endpoint limits. Uses defaults if not provided.
        """
        super().__init__(http_client, "/models/datamodels", limits)

    def _page_response(self, response: SuccessResponse) -> Page[DataModelResponse]:
        return Page[DataModelResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ReferenceResponseItems[DataModelReference]:
        return ReferenceResponseItems[DataModelReference].model_validate_json(response.body)

    def create(self, items: Sequence[DataModelRequest]) -> list[DataModelResponse]:
        """Create or update data models.

        Args:
            items: A sequence of request objects defining the data models to create/update.

        Returns:
            A list of the created/updated data model objects.
        """
        return self._create(items)

    @overload
    def retrieve(
        self,
        references: Sequence[DataModelReference],
        inline_views: Literal[False] = False,
    ) -> list[DataModelResponse]: ...

    @overload
    def retrieve(
        self,
        references: Sequence[DataModelReference],
        inline_views: Literal[True],
    ) -> list[DataModelResponseWithViews]: ...

    def retrieve(
        self,
        references: Sequence[DataModelReference],
        inline_views: bool = False,
    ) -> list[DataModelResponse] | list[DataModelResponseWithViews]:
        """Retrieve specific data models by their references.

        Args:
            references: A sequence of reference objects identifying the data models to retrieve.
            inline_views: Whether to include the full view definitions inline (default: False)

        Returns:
            A list of data model objects. Data models that don't exist are not included.
        """
        if inline_views is False:
            return self._retrieve(references)
        else:
            return self._retrieve_with_views(references)

    def _retrieve_with_views(self, references: Sequence[DataModelReference]) -> list[DataModelResponseWithViews]:
        if not references:
            return []
        all_items: list[DataModelResponseWithViews] = []
        for chunk in chunker_sequence(references, self._limits.retrieve):
            request = RequestMessage(
                endpoint_url=f"{self._make_url()}/byids",
                method="POST",
                body_content={"items": self._serialize_reference(chunk)},  # type: ignore[dict-item]
                parameters={"inlineViews": True},
            )
            result = self._http_client.request_with_retries(request)
            response = result.get_success_or_raise()
            items = Page[DataModelResponseWithViews].model_validate_json(response.body).items
            all_items.extend(items)

        return all_items

    def delete(self, references: Sequence[DataModelReference]) -> list[DataModelReference]:
        """Delete data models by their references.

        Args:
            references: A sequence of reference objects identifying the data models to delete.

        Returns:
            A list of references to the deleted data models.
        """
        return self._delete(references)

    def iterate(
        self,
        space: str | None = None,
        include_global: bool = False,
        all_versions: bool = False,
        inline_views: bool = False,
        limit: int = 10,
        cursor: str | None = None,
    ) -> Page[DataModelResponse]:
        """Fetch a single page of data models.

        Args:
            space: Filter by space (default: None, meaning all spaces)
            include_global: Whether to include global data models (default: False)
            all_versions: Whether to return all versions (default: False)
            inline_views: Whether to include view definitions inline (default: False)
            limit: Maximum number of items per page (default: 10)
            cursor: Cursor for pagination (default: None)

        Returns:
            A Page containing the data models and the cursor for the next page.
        """
        return self._iterate(
            limit,
            cursor,
            {
                "space": space,
                "includeGlobal": include_global,
                "allVersions": all_versions,
                "inlineViews": inline_views,
            },
        )

    def list(
        self,
        space: str | None = None,
        include_global: bool = False,
        all_versions: bool = False,
        inline_views: bool = False,
        limit: int | None = 10,
    ) -> list[DataModelResponse]:
        """List all data models, handling pagination automatically.

        This method fetches all data models, handling pagination transparently.

        Args:
            space: Filter by space (default: None, meaning all spaces)
            include_global: Whether to include global data models (default: False)
            all_versions: Whether to return all versions (default: False)
            inline_views: Whether to include view definitions inline (default: False)
            limit: Maximum number of items to retrieve (default: None, meaning no limit)

        Returns:
            A list of all data model objects.
        """
        return self._list(
            limit,
            {
                "space": space,
                "includeGlobal": include_global,
                "allVersions": all_versions,
                "inlineViews": inline_views,
            },
        )
