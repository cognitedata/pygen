"""Data Models API resource client.

This module provides the DataModelsAPI class for managing CDF data models.
"""

from collections.abc import Iterator, Sequence

from cognite.pygen._client.http_client import HTTPClient
from cognite.pygen._client.models import DataModelReference, DataModelRequest, DataModelResponse

from ._base import BaseResourceAPI, Page, ResourceLimits


class DataModelsAPI:
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
        >>> models = client.data_models.retrieve([ref], params={"inlineViews": True})
    """

    def __init__(self, http_client: HTTPClient, limits: ResourceLimits | None = None) -> None:
        """Initialize the Data Models API.

        Args:
            http_client: The HTTP client to use for API requests.
            limits: Configuration for API endpoint limits. Uses defaults if not provided.
        """
        self._api = BaseResourceAPI[DataModelReference, DataModelResponse](
            http_client=http_client,
            endpoint="/models/datamodels",
            reference_cls=DataModelReference,
            request_cls=DataModelRequest,
            response_cls=DataModelResponse,
            limits=limits,
        )

    def create(self, items: Sequence[DataModelRequest]) -> list[DataModelResponse]:
        """Create or update data models.

        Args:
            items: A sequence of request objects defining the data models to create/update.

        Returns:
            A list of the created/updated data model objects.
        """
        return self._api.create(items)

    def retrieve(
        self,
        references: Sequence[DataModelReference],
        params: dict | None = None,
    ) -> list[DataModelResponse]:
        """Retrieve specific data models by their references.

        Args:
            references: A sequence of reference objects identifying the data models to retrieve.
            params: Additional query parameters. Supported parameters:
                - inlineViews: Whether to include the full view definitions inline (default: False)

        Returns:
            A list of data model objects. Data models that don't exist are not included.
        """
        return self._api.retrieve(references, params=params)

    def delete(self, references: Sequence[DataModelReference]) -> list[DataModelReference]:
        """Delete data models by their references.

        Args:
            references: A sequence of reference objects identifying the data models to delete.

        Returns:
            A list of references to the deleted data models.
        """
        return self._api.delete(references)

    def iterate(
        self,
        params: dict | None = None,
    ) -> Page[DataModelResponse]:
        """Fetch a single page of data models.

        Args:
            params: Query parameters for the request. Supported parameters:
                - cursor: Cursor for pagination
                - limit: Maximum number of items per page (default: 1000)
                - space: Filter by space
                - includeGlobal: Whether to include global data models (default: False)
                - allVersions: Whether to return all versions (default: False)
                - inlineViews: Whether to include view definitions inline (default: False)

        Returns:
            A Page containing the data models and the cursor for the next page.
        """
        return self._api.iterate(params)

    def list(
        self,
        params: dict | None = None,
    ) -> Iterator[DataModelResponse]:
        """List all data models, handling pagination automatically.

        This method lazily iterates over all data models, fetching pages as needed.

        Args:
            params: Query parameters for the request. Supported parameters:
                - space: Filter by space
                - includeGlobal: Whether to include global data models (default: False)
                - limit: Maximum total number of items to return
                - allVersions: Whether to return all versions (default: False)
                - inlineViews: Whether to include view definitions inline (default: False)

        Yields:
            DataModelResponse objects from the API.
        """
        return self._api.list(params)
