"""Data Models API resource client.

This module provides the DataModelsAPI class for managing CDF data models.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence

from pydantic import TypeAdapter

from cognite.pygen._client.http_client import HTTPClient, RequestMessage
from cognite.pygen._client.models import DataModelReference, DataModelRequest, DataModelResponse, ViewResponse

from ._base import BaseResourceAPI, Page


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

    def __init__(self, http_client: HTTPClient) -> None:
        """Initialize the Data Models API.

        Args:
            http_client: The HTTP client to use for API requests.
        """
        super().__init__(http_client)
        self._response_type_adapter = TypeAdapter(list[DataModelResponse])
        self._reference_type_adapter = TypeAdapter(list[DataModelReference])
        self._request_type_adapter = TypeAdapter(list[DataModelRequest])
        self._view_response_type_adapter = TypeAdapter(list[ViewResponse])

    @property
    def _endpoint(self) -> str:
        return "/models/datamodels"

    @property
    def _response_adapter(self) -> TypeAdapter[list[DataModelResponse]]:
        return self._response_type_adapter

    @property
    def _reference_adapter(self) -> TypeAdapter[list[DataModelReference]]:
        return self._reference_type_adapter

    @property
    def _request_adapter(self) -> TypeAdapter[list[DataModelRequest]]:
        return self._request_type_adapter

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
        params: dict[str, str | int | bool] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        if space is not None:
            params["space"] = space
        if include_global:
            params["includeGlobal"] = True
        if all_versions:
            params["allVersions"] = True
        if inline_views:
            params["inlineViews"] = True

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
                inline_views=inline_views,
            )

            for item in page.items:
                yield item
                count += 1
                if limit is not None and count >= limit:
                    return

            if page.cursor is None:
                break
            cursor = page.cursor

    def retrieve(
        self,
        references: Sequence[DataModelReference],
        inline_views: bool = False,
    ) -> list[DataModelResponse]:
        """Retrieve specific data models by their references.

        Args:
            references: A sequence of reference objects identifying the data models to retrieve.
            inline_views: Whether to include the full view definitions inline. Default is False.

        Returns:
            A list of data model objects. Data models that don't exist are not included.
        """
        if not references:
            return []

        body = {"items": self._reference_adapter.dump_python(list(references), mode="json", by_alias=True)}

        params: dict[str, str | int | bool] = {}
        if inline_views:
            params["inlineViews"] = True

        request = RequestMessage(
            endpoint_url=f"{self._make_url()}/byids",
            method="POST",
            body_content=body,
            parameters=params if params else None,
        )

        result = self._http_client.request_with_retries(request)
        response = result.get_success_or_raise()

        body_json = response.body_json
        items_data = body_json.get("items", [])
        return self._response_adapter.validate_python(items_data)
