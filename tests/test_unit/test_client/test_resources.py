"""Unit tests for resource clients."""

from collections.abc import Iterator
from typing import Any

import pytest
import respx
from httpx import Response

from cognite.pygen._client import (
    Page,
    PygenClient,
    PygenClientConfig,
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


class TestSpacesAPI:
    def test_iterate_returns_page(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_space_resource: dict[str, Any],
    ) -> None:
        client = pygen_client
        config = client.config
        respx_mock.get(config.create_api_url("/models/spaces")).respond(
            json={"items": [example_space_resource], "nextCursor": None}
        )
        page = client.spaces.iterate(limit=100)
        assert isinstance(page, Page)
        assert len(page.items) == 1
        assert page.cursor is None
        assert all(isinstance(item, SpaceResponse) for item in page.items)
        assert page.items[0].space == example_space_resource["space"]

    def test_iterate_with_pagination(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_space_resource: dict[str, Any],
    ) -> None:
        respx_mock.get("https://example.com/api/v1/projects/test_project/models/spaces").respond(
            json={"items": [example_space_resource], "nextCursor": "cursor123"}
        )
        page = pygen_client.spaces.iterate(limit=1)
        assert page.cursor == "cursor123"
        assert len(page.items) == 1

    def test_list_iterates_all_pages(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_space_resource: dict[str, Any],
    ) -> None:
        # Use side_effect to return different responses for consecutive calls
        space1 = {**example_space_resource, "space": "space1"}
        space2 = {**example_space_resource, "space": "space2"}
        responses = iter(
            [
                Response(200, json={"items": [space1], "nextCursor": "cursor123"}),
                Response(200, json={"items": [space2], "nextCursor": None}),
            ]
        )
        respx_mock.get("https://example.com/api/v1/projects/test_project/models/spaces").mock(
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
        example_space_resource: dict[str, Any],
    ) -> None:
        # The list method fetches pages until it has at least `limit` items,
        # then returns all items from fetched pages (may exceed limit).
        # This test verifies basic list functionality with a limit parameter.
        respx_mock.get("https://example.com/api/v1/projects/test_project/models/spaces").respond(
            json={
                "items": [{**example_space_resource, "space": f"space{i}"} for i in range(5)],
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
        example_space_resource: dict[str, Any],
    ) -> None:
        respx_mock.post("https://example.com/api/v1/projects/test_project/models/spaces/byids").respond(
            json={"items": [example_space_resource]}
        )
        spaces = pygen_client.spaces.retrieve([SpaceReference(space=example_space_resource["space"])])
        assert len(spaces) == 1
        assert spaces[0].space == example_space_resource["space"]

    def test_retrieve_empty_list(self, pygen_client: PygenClient) -> None:
        spaces = pygen_client.spaces.retrieve([])
        assert spaces == []

    def test_create_space(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_space_resource: dict[str, Any],
    ) -> None:
        respx_mock.post("https://example.com/api/v1/projects/test_project/models/spaces").respond(
            json={"items": [example_space_resource]}
        )
        request = SpaceRequest(
            space=example_space_resource["space"],
            name=example_space_resource["name"],
            description=example_space_resource["description"],
        )
        created = pygen_client.spaces.create([request])
        assert len(created) == 1
        assert created[0].space == example_space_resource["space"]
        assert created[0].name == example_space_resource["name"]

    def test_create_empty_list(self, pygen_client: PygenClient) -> None:
        created = pygen_client.spaces.create([])
        assert created == []

    def test_delete_space(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_space_resource: dict[str, Any],
    ) -> None:
        respx_mock.post("https://example.com/api/v1/projects/test_project/models/spaces/delete").respond(
            json={"items": [{"space": example_space_resource["space"]}]}
        )
        deleted = pygen_client.spaces.delete([SpaceReference(space=example_space_resource["space"])])
        assert len(deleted) == 1
        assert deleted[0].space == example_space_resource["space"]

    def test_delete_empty_list(self, pygen_client: PygenClient) -> None:
        deleted = pygen_client.spaces.delete([])
        assert deleted == []


class TestDataModelsAPI:
    def test_iterate_returns_page(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_data_model_resource: dict[str, Any],
    ) -> None:
        respx_mock.get("https://example.com/api/v1/projects/test_project/models/datamodels").respond(
            json={"items": [example_data_model_resource], "nextCursor": None}
        )
        page = pygen_client.data_models.iterate(limit=100)
        assert isinstance(page, Page)
        assert len(page.items) == 1
        assert page.items[0].external_id == example_data_model_resource["externalId"]
        assert page.items[0].views is not None
        assert len(page.items[0].views) == 1

    def test_retrieve_with_inline_views(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_data_model_resource: dict[str, Any],
    ) -> None:
        response_model = {**example_data_model_resource, "views": []}
        route = respx_mock.post("https://example.com/api/v1/projects/test_project/models/datamodels/byids").respond(
            json={"items": [response_model]}
        )
        ref = DataModelReference(
            space=example_data_model_resource["space"],
            external_id=example_data_model_resource["externalId"],
            version=example_data_model_resource["version"],
        )
        models = pygen_client.data_models.retrieve([ref], inline_views=True)
        assert len(models) == 1
        # Check that inlineViews parameter was sent (lowercase 'true' in URL)
        request = route.calls[0].request
        assert "inlineViews=true" in str(request.url)

    def test_list_with_all_versions(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_data_model_resource: dict[str, Any],
    ) -> None:
        model_v1 = {**example_data_model_resource, "version": "v1", "views": None}
        model_v2 = {**example_data_model_resource, "version": "v2", "views": None}
        route = respx_mock.get("https://example.com/api/v1/projects/test_project/models/datamodels").respond(
            json={"items": [model_v1, model_v2], "nextCursor": None}
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
        example_data_model_resource: dict[str, Any],
    ) -> None:
        response_model = {**example_data_model_resource, "views": None}
        respx_mock.post("https://example.com/api/v1/projects/test_project/models/datamodels").respond(
            json={"items": [response_model]}
        )
        request = DataModelRequest(
            space=example_data_model_resource["space"],
            external_id=example_data_model_resource["externalId"],
            version=example_data_model_resource["version"],
        )
        created = pygen_client.data_models.create([request])
        assert len(created) == 1
        assert created[0].external_id == example_data_model_resource["externalId"]


class TestViewsAPI:
    def test_iterate_returns_page(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_view_resource: dict[str, Any],
    ) -> None:
        respx_mock.get("https://example.com/api/v1/projects/test_project/models/views").respond(
            json={"items": [example_view_resource], "nextCursor": None}
        )
        page = pygen_client.views.iterate(limit=100)
        assert isinstance(page, Page)
        assert len(page.items) == 1
        assert page.items[0].external_id == example_view_resource["externalId"]

    def test_list_with_include_inherited_properties(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_view_resource: dict[str, Any],
    ) -> None:
        route = respx_mock.get("https://example.com/api/v1/projects/test_project/models/views").respond(
            json={"items": [example_view_resource], "nextCursor": None}
        )
        views = pygen_client.views.list(include_inherited_properties=False)
        assert len(views) == 1
        # Check that includeInheritedProperties parameter was sent (lowercase 'false' in URL)
        request = route.calls[0].request
        assert "includeInheritedProperties=false" in str(request.url)

    def test_retrieve_views(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_view_resource: dict[str, Any],
    ) -> None:
        respx_mock.post("https://example.com/api/v1/projects/test_project/models/views/byids").respond(
            json={"items": [example_view_resource]}
        )
        ref = ViewReference(
            space=example_view_resource["space"],
            external_id=example_view_resource["externalId"],
            version=example_view_resource["version"],
        )
        views = pygen_client.views.retrieve([ref])
        assert len(views) == 1
        assert views[0].external_id == example_view_resource["externalId"]


class TestContainersAPI:
    def test_iterate_returns_page(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_container_resource: dict[str, Any],
    ) -> None:
        respx_mock.get("https://example.com/api/v1/projects/test_project/models/containers").respond(
            json={"items": [example_container_resource], "nextCursor": None}
        )
        page = pygen_client.containers.iterate(limit=100)
        assert isinstance(page, Page)
        assert len(page.items) == 1
        assert page.items[0].external_id == example_container_resource["externalId"]

    def test_list_containers(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_container_resource: dict[str, Any],
    ) -> None:
        respx_mock.get("https://example.com/api/v1/projects/test_project/models/containers").respond(
            json={"items": [example_container_resource], "nextCursor": None}
        )
        containers = list(pygen_client.containers.list())
        assert len(containers) == 1
        assert containers[0].external_id == example_container_resource["externalId"]

    def test_retrieve_containers(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_container_resource: dict[str, Any],
    ) -> None:
        respx_mock.post("https://example.com/api/v1/projects/test_project/models/containers/byids").respond(
            json={"items": [example_container_resource]}
        )
        ref = ContainerReference(
            space=example_container_resource["space"], external_id=example_container_resource["externalId"]
        )
        containers = pygen_client.containers.retrieve([ref])
        assert len(containers) == 1
        assert containers[0].external_id == example_container_resource["externalId"]

    def test_delete_container(
        self,
        pygen_client: PygenClient,
        respx_mock: respx.MockRouter,
        example_container_resource: dict[str, Any],
    ) -> None:
        respx_mock.post("https://example.com/api/v1/projects/test_project/models/containers/delete").respond(
            json={
                "items": [
                    {
                        "space": example_container_resource["space"],
                        "externalId": example_container_resource["externalId"],
                        "type": "container",
                    }
                ]
            }
        )
        deleted = pygen_client.containers.delete(
            [
                ContainerReference(
                    space=example_container_resource["space"], external_id=example_container_resource["externalId"]
                )
            ]
        )
        assert len(deleted) == 1
        assert deleted[0].external_id == example_container_resource["externalId"]
