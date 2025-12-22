"""Data Models API resource client.

This module provides the DataModelsAPI class for managing CDF data models.
"""

from __future__ import annotations

import builtins
from collections.abc import Iterator, Sequence

from cognite.pygen._client.http_client import HTTPClient
from cognite.pygen._client.models import DataModelReference, DataModelRequest, DataModelResponse

from ._base import BaseResourceAPI, Page


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
        >>> models = client.data_models.retrieve([ref], inline_views=True)
    """

    def __init__(self, http_client: HTTPClient) -> None:
        """Initialize the Data Models API.

        Args:
            http_client: The HTTP client to use for API requests.
        """
        self._api = BaseResourceAPI[DataModelReference, DataModelResponse](
            http_client=http_client,
            endpoint="/models/datamodels",
            reference_cls=DataModelReference,
            request_cls=DataModelRequest,
            response_cls=DataModelResponse,
        )

    def iterate(
        self,
        *,
        space: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
        include_global: bool = False,
        all_versions: bool = False,
        inline_views: bool = False,
    ) -> Page[DataModelResponse]:
        """Fetch a single page of data models.

        Args:
            space: Filter by space. If None, returns data models from all spaces.
            cursor: Cursor for pagination. If None, starts from the beginning.
            limit: Maximum number of items to return per page. Default is 100.
            include_global: Whether to include global data models. Default is False.
            all_versions: Whether to return all versions of each data model. Default is False.
            inline_views: Whether to include the full view definitions inline. Default is False.

        Returns:
            A Page containing the data models and the cursor for the next page.
        """
        extra_params: dict[str, str | int | bool] = {}
        if all_versions:
            extra_params["allVersions"] = True
        if inline_views:
            extra_params["inlineViews"] = True

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
        inline_views: bool = False,
    ) -> Iterator[DataModelResponse]:
        """List all data models, handling pagination automatically.

        This method lazily iterates over all data models, fetching pages as needed.

        Args:
            space: Filter by space. If None, returns data models from all spaces.
            include_global: Whether to include global data models. Default is False.
            limit: Maximum total number of items to return. If None, returns all items.
            all_versions: Whether to return all versions of each data model. Default is False.
            inline_views: Whether to include the full view definitions inline. Default is False.

        Yields:
            DataModelResponse objects from the API.
        """
        extra_params: dict[str, str | int | bool] = {}
        if all_versions:
            extra_params["allVersions"] = True
        if inline_views:
            extra_params["inlineViews"] = True

        return self._api.list(
            space=space,
            include_global=include_global,
            limit=limit,
            extra_params=extra_params if extra_params else None,
        )

    def retrieve(
        self,
        references: Sequence[DataModelReference],
        inline_views: bool = False,
    ) -> builtins.list[DataModelResponse]:
        """Retrieve specific data models by their references.

        Args:
            references: A sequence of reference objects identifying the data models to retrieve.
            inline_views: Whether to include the full view definitions inline. Default is False.

        Returns:
            A list of data model objects. Data models that don't exist are not included.
        """
        extra_params: dict[str, str | int | bool] | None = None
        if inline_views:
            extra_params = {"inlineViews": True}

        return self._api.retrieve(references, extra_params=extra_params)

    def create(self, items: Sequence[DataModelRequest]) -> builtins.list[DataModelResponse]:
        """Create or update data models.

        Args:
            items: A sequence of request objects defining the data models to create/update.

        Returns:
            A list of the created/updated data model objects.
        """
        return self._api.create(items)

    def delete(self, references: Sequence[DataModelReference]) -> builtins.list[DataModelReference]:
        """Delete data models by their references.

        Args:
            references: A sequence of reference objects identifying the data models to delete.

        Returns:
            A list of references to the deleted data models.
        """
        return self._api.delete(references)
