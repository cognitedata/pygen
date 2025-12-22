"""Data Models API resource client.

This module provides the DataModelsAPI class for managing CDF data models.
"""

from collections.abc import Sequence

from cognite.pygen._client.http_client import HTTPClient, SuccessResponse
from cognite.pygen._client.models import DataModelReference, DataModelRequest, DataModelResponse

from ._base import BaseResourceAPI, Page, ReferenceResponseItems, ResourceLimits


class DataModelsAPI(BaseResourceAPI[DataModelReference, DataModelRequest, DataModelResponse]):
    """API client for CDF Data Model resources.

    Data models group and structure views into reusable collections.
    A data model contains a set of views where the node types can
    refer to each other with direct relations and edges.

    Example:
        >>> from cognite.pygen._client import PygenClient
        >>> client = PygenClient(config)
        >>> # List all data models
        >>> for model in client.data_models.list():
        ...     print(f"{model.space}:{model.external_id}")
        >>> # Retrieve specific data models with inline views
        >>> ref = DataModelReference(space="my_space", external_id="my_model", version="v1")
        >>> models = client.data_models.retrieve([ref], inline_views=True)
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

    def retrieve(
        self,
        references: Sequence[DataModelReference],
        inline_views: bool = False,
    ) -> list[DataModelResponse]:
        """Retrieve specific data models by their references.

        Args:
            references: A sequence of reference objects identifying the data models to retrieve.
            inline_views: Whether to include the full view definitions inline (default: False)

        Returns:
            A list of data model objects. Data models that don't exist are not included.
        """
        return self._retrieve(references, params={"inlineViews": inline_views})

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
