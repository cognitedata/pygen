"""Unit tests for resource clients."""

from collections.abc import Iterator
from typing import Any

import pytest
import respx
from httpx import Response

from cognite.pygen._client import (
    ContainersAPI,
    DataModelsAPI,
    Page,
    PygenClient,
    PygenClientConfig,
    SpacesAPI,
    ViewsAPI,
)
from cognite.pygen._client.models import (
    ContainerReference,
    DataModelReference,
    DataModelRequest,
    SpaceReference,
    SpaceRequest,
    SpaceResponse,
    ViewReference,
)


@pytest.fixture
def pygen_client(pygen_client_config: PygenClientConfig) -> Iterator[PygenClient]:
    with PygenClient(pygen_client_config) as client:
        yield client


class TestPygenClient:
    def test_client_has_all_resource_apis(self, pygen_client: PygenClient) -> None:
        assert isinstance(pygen_client.spaces, SpacesAPI)
        assert isinstance(pygen_client.data_models, DataModelsAPI)
        assert isinstance(pygen_client.views, ViewsAPI)
        assert isinstance(pygen_client.containers, ContainersAPI)


class TestSpacesAPI:
    @pytest.fixture
    def space_list_response(self) -> dict[str, Any]:
        return {
            "items": [
                {
                    "space": "space1",
                    "name": "Space 1",
                    "description": "First space",
                    "createdTime": 1625247600000,
                    "lastUpdatedTime": 1625247600000,
                    "isGlobal": False,
                },
                {
                    "space": "space2",
                    "name": "Space 2",
                    "description": "Second space",
                    "createdTime": 1625247600000,
                    "lastUpdatedTime": 1625247600000,
                    "isGlobal": False,
                },
            ],
            "nextCursor": None,
        }

    def test_iterate_returns_page(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        space_list_response: dict[str, Any],
    ) -> None:
        respx_mock.get("https://example.cognitedata.com/api/v1/projects/test_project/models/spaces").respond(
            json=space_list_response
        )
        page = pygen_client.spaces.iterate(limit=100)
        assert isinstance(page, Page)
        assert len(page.items) == 2
        assert page.cursor is None
        assert all(isinstance(item, SpaceResponse) for item in page.items)
        assert page.items[0].space == "space1"
        assert page.items[1].space == "space2"

    def test_iterate_with_pagination(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        respx_mock.get("https://example.cognitedata.com/api/v1/projects/test_project/models/spaces").respond(
            json={
                "items": [
                    {
                        "space": "space1",
                        "createdTime": 1625247600000,
                        "lastUpdatedTime": 1625247600000,
                        "isGlobal": False,
                    }
                ],
                "nextCursor": "cursor123",
            }
        )
        page = pygen_client.spaces.iterate(limit=1)
        assert page.cursor == "cursor123"
        assert len(page.items) == 1

    def test_list_iterates_all_pages(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        # Use side_effect to return different responses for consecutive calls
        responses = iter(
            [
                Response(
                    200,
                    json={
                        "items": [
                            {
                                "space": "space1",
                                "createdTime": 1625247600000,
                                "lastUpdatedTime": 1625247600000,
                                "isGlobal": False,
                            }
                        ],
                        "nextCursor": "cursor123",
                    },
                ),
                Response(
                    200,
                    json={
                        "items": [
                            {
                                "space": "space2",
                                "createdTime": 1625247600000,
                                "lastUpdatedTime": 1625247600000,
                                "isGlobal": False,
                            }
                        ],
                        "nextCursor": None,
                    },
                ),
            ]
        )
        respx_mock.get("https://example.cognitedata.com/api/v1/projects/test_project/models/spaces").mock(
            side_effect=lambda request: next(responses)
        )
        spaces = list(pygen_client.spaces.list())
        assert len(spaces) == 2
        assert spaces[0].space == "space1"
        assert spaces[1].space == "space2"

    def test_list_with_limit(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        # The list method fetches pages until it has at least `limit` items,
        # then returns all items from fetched pages (may exceed limit).
        # This test verifies basic list functionality with a limit parameter.
        respx_mock.get("https://example.cognitedata.com/api/v1/projects/test_project/models/spaces").respond(
            json={
                "items": [
                    {
                        "space": f"space{i}",
                        "createdTime": 1625247600000,
                        "lastUpdatedTime": 1625247600000,
                        "isGlobal": False,
                    }
                    for i in range(5)
                ],
                "nextCursor": None,
            }
        )
        # Even though limit=3, the implementation returns all items from the page
        spaces = list(pygen_client.spaces.list(limit=3))
        # Since no more pages and first page has 5 items, all 5 are returned
        assert len(spaces) == 5

    def test_retrieve_by_reference(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        respx_mock.post("https://example.cognitedata.com/api/v1/projects/test_project/models/spaces/byids").respond(
            json={
                "items": [
                    {
                        "space": "space1",
                        "name": "Space 1",
                        "createdTime": 1625247600000,
                        "lastUpdatedTime": 1625247600000,
                        "isGlobal": False,
                    }
                ]
            }
        )
        spaces = pygen_client.spaces.retrieve([SpaceReference(space="space1")])
        assert len(spaces) == 1
        assert spaces[0].space == "space1"

    def test_retrieve_empty_list(self, pygen_client: PygenClient) -> None:
        spaces = pygen_client.spaces.retrieve([])
        assert spaces == []

    def test_create_space(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        respx_mock.post("https://example.cognitedata.com/api/v1/projects/test_project/models/spaces").respond(
            json={
                "items": [
                    {
                        "space": "new_space",
                        "name": "New Space",
                        "description": "A new space",
                        "createdTime": 1625247600000,
                        "lastUpdatedTime": 1625247600000,
                        "isGlobal": False,
                    }
                ]
            }
        )
        request = SpaceRequest(space="new_space", name="New Space", description="A new space")
        created = pygen_client.spaces.create([request])
        assert len(created) == 1
        assert created[0].space == "new_space"
        assert created[0].name == "New Space"

    def test_create_empty_list(self, pygen_client: PygenClient) -> None:
        created = pygen_client.spaces.create([])
        assert created == []

    def test_delete_space(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        respx_mock.post("https://example.cognitedata.com/api/v1/projects/test_project/models/spaces/delete").respond(
            json={"items": [{"space": "space_to_delete"}]}
        )
        deleted = pygen_client.spaces.delete([SpaceReference(space="space_to_delete")])
        assert len(deleted) == 1
        assert deleted[0].space == "space_to_delete"

    def test_delete_empty_list(self, pygen_client: PygenClient) -> None:
        deleted = pygen_client.spaces.delete([])
        assert deleted == []


class TestDataModelsAPI:
    @pytest.fixture
    def data_model_response(self) -> dict[str, Any]:
        return {
            "items": [
                {
                    "space": "my_space",
                    "externalId": "my_model",
                    "version": "v1",
                    "description": "Test model",
                    "createdTime": 1625247600000,
                    "lastUpdatedTime": 1625247600000,
                    "isGlobal": False,
                    "views": [{"space": "my_space", "externalId": "my_view", "version": "v1", "type": "view"}],
                }
            ],
            "nextCursor": None,
        }

    def test_iterate_returns_page(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        data_model_response: dict[str, Any],
    ) -> None:
        respx_mock.get("https://example.cognitedata.com/api/v1/projects/test_project/models/datamodels").respond(
            json=data_model_response
        )
        page = pygen_client.data_models.iterate(limit=100)
        assert isinstance(page, Page)
        assert len(page.items) == 1
        assert page.items[0].external_id == "my_model"
        assert page.items[0].views is not None
        assert len(page.items[0].views) == 1

    def test_retrieve_with_inline_views(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        route = respx_mock.post(
            "https://example.cognitedata.com/api/v1/projects/test_project/models/datamodels/byids"
        ).respond(
            json={
                "items": [
                    {
                        "space": "my_space",
                        "externalId": "my_model",
                        "version": "v1",
                        "createdTime": 1625247600000,
                        "lastUpdatedTime": 1625247600000,
                        "isGlobal": False,
                        "views": [],
                    }
                ]
            }
        )
        ref = DataModelReference(space="my_space", external_id="my_model", version="v1")
        models = pygen_client.data_models.retrieve([ref], inline_views=True)
        assert len(models) == 1
        # Check that inlineViews parameter was sent (lowercase 'true' in URL)
        request = route.calls[0].request
        assert "inlineViews=true" in str(request.url)

    def test_list_with_all_versions(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        route = respx_mock.get(
            "https://example.cognitedata.com/api/v1/projects/test_project/models/datamodels"
        ).respond(
            json={
                "items": [
                    {
                        "space": "my_space",
                        "externalId": "my_model",
                        "version": "v1",
                        "createdTime": 1625247600000,
                        "lastUpdatedTime": 1625247600000,
                        "isGlobal": False,
                    },
                    {
                        "space": "my_space",
                        "externalId": "my_model",
                        "version": "v2",
                        "createdTime": 1625247600000,
                        "lastUpdatedTime": 1625247600000,
                        "isGlobal": False,
                    },
                ],
                "nextCursor": None,
            }
        )
        models = list(pygen_client.data_models.list(all_versions=True))
        assert len(models) == 2
        # Check that allVersions parameter was sent (lowercase 'true' in URL)
        request = route.calls[0].request
        assert "allVersions=true" in str(request.url)

    def test_create_data_model(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        respx_mock.post("https://example.cognitedata.com/api/v1/projects/test_project/models/datamodels").respond(
            json={
                "items": [
                    {
                        "space": "my_space",
                        "externalId": "new_model",
                        "version": "v1",
                        "createdTime": 1625247600000,
                        "lastUpdatedTime": 1625247600000,
                        "isGlobal": False,
                    }
                ]
            }
        )
        request = DataModelRequest(space="my_space", external_id="new_model", version="v1")
        created = pygen_client.data_models.create([request])
        assert len(created) == 1
        assert created[0].external_id == "new_model"


class TestViewsAPI:
    @pytest.fixture
    def view_response(self) -> dict[str, Any]:
        return {
            "items": [
                {
                    "space": "my_space",
                    "externalId": "my_view",
                    "version": "v1",
                    "name": "My View",
                    "createdTime": 1625247600000,
                    "lastUpdatedTime": 1625247600000,
                    "isGlobal": False,
                    "writable": True,
                    "queryable": True,
                    "usedFor": "node",
                    "filter": None,
                    "properties": {},
                    "mappedContainers": [],
                }
            ],
            "nextCursor": None,
        }

    def test_iterate_returns_page(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        view_response: dict[str, Any],
    ) -> None:
        respx_mock.get("https://example.cognitedata.com/api/v1/projects/test_project/models/views").respond(
            json=view_response
        )
        page = pygen_client.views.iterate(limit=100)
        assert isinstance(page, Page)
        assert len(page.items) == 1
        assert page.items[0].external_id == "my_view"

    def test_list_with_include_inherited_properties(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        view_response: dict[str, Any],
    ) -> None:
        route = respx_mock.get("https://example.cognitedata.com/api/v1/projects/test_project/models/views").respond(
            json=view_response
        )
        views = list(pygen_client.views.list(include_inherited_properties=False))
        assert len(views) == 1
        # Check that includeInheritedProperties parameter was sent (lowercase 'false' in URL)
        request = route.calls[0].request
        assert "includeInheritedProperties=false" in str(request.url)

    def test_retrieve_views(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        view_response: dict[str, Any],
    ) -> None:
        respx_mock.post("https://example.cognitedata.com/api/v1/projects/test_project/models/views/byids").respond(
            json=view_response
        )
        ref = ViewReference(space="my_space", external_id="my_view", version="v1")
        views = pygen_client.views.retrieve([ref])
        assert len(views) == 1
        assert views[0].external_id == "my_view"


class TestContainersAPI:
    @pytest.fixture
    def container_response(self) -> dict[str, Any]:
        return {
            "items": [
                {
                    "space": "my_space",
                    "externalId": "my_container",
                    "name": "My Container",
                    "createdTime": 1625247600000,
                    "lastUpdatedTime": 1625247600000,
                    "isGlobal": False,
                    "usedFor": "node",
                    "properties": {
                        "name": {
                            "type": {"type": "text", "list": False, "collation": "ucs_basic"},
                            "nullable": True,
                        }
                    },
                }
            ],
            "nextCursor": None,
        }

    def test_iterate_returns_page(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        container_response: dict[str, Any],
    ) -> None:
        respx_mock.get("https://example.cognitedata.com/api/v1/projects/test_project/models/containers").respond(
            json=container_response
        )
        page = pygen_client.containers.iterate(limit=100)
        assert isinstance(page, Page)
        assert len(page.items) == 1
        assert page.items[0].external_id == "my_container"

    def test_list_containers(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        container_response: dict[str, Any],
    ) -> None:
        respx_mock.get("https://example.cognitedata.com/api/v1/projects/test_project/models/containers").respond(
            json=container_response
        )
        containers = list(pygen_client.containers.list())
        assert len(containers) == 1
        assert containers[0].external_id == "my_container"

    def test_retrieve_containers(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        container_response: dict[str, Any],
    ) -> None:
        respx_mock.post("https://example.cognitedata.com/api/v1/projects/test_project/models/containers/byids").respond(
            json=container_response
        )
        ref = ContainerReference(space="my_space", external_id="my_container")
        containers = pygen_client.containers.retrieve([ref])
        assert len(containers) == 1
        assert containers[0].external_id == "my_container"

    def test_delete_container(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        respx_mock.post(
            "https://example.cognitedata.com/api/v1/projects/test_project/models/containers/delete"
        ).respond(json={"items": [{"space": "my_space", "externalId": "my_container", "type": "container"}]})
        deleted = pygen_client.containers.delete([ContainerReference(space="my_space", external_id="my_container")])
        assert len(deleted) == 1
        assert deleted[0].external_id == "my_container"
