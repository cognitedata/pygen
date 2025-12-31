"""Tests for the Example SDK (ProductNode, CategoryNode, RelatesTo)."""

from collections import UserList
from collections.abc import Iterator
from typing import Any, ClassVar

import pytest
import respx

from cognite.pygen._python.example import (
    CategoryNode,
    ExampleClient,
    ProductNode,
    RelatesTo,
)
from cognite.pygen._python.example._api import CategoryNodeAPI, ProductNodeAPI, RelatesToAPI
from cognite.pygen._python.instance_api import (
    AggregatedNumberValue,
    AggregateResponse,
    Count,
    Instance,
    InstanceAPI,
    InstanceWrite,
)
from cognite.pygen._python.instance_api.config import PygenClientConfig


@pytest.fixture
def example_client(pygen_client_config: PygenClientConfig) -> Iterator[ExampleClient]:
    """Create an ExampleClient for testing."""
    with ExampleClient(pygen_client_config) as client:
        yield client


@pytest.fixture
def list_url(pygen_client_config: PygenClientConfig) -> str:
    """Return the URL for listing instances."""
    return pygen_client_config.create_api_url("/models/instances/list")


@pytest.fixture
def retrieve_url(pygen_client_config: PygenClientConfig) -> str:
    """Return the URL for retrieving instances."""
    return pygen_client_config.create_api_url("/models/instances/byids")


@pytest.fixture
def search_url(pygen_client_config: PygenClientConfig) -> str:
    """Return the URL for searching instances."""
    return pygen_client_config.create_api_url("/models/instances/search")


@pytest.fixture
def aggregate_url(pygen_client_config: PygenClientConfig) -> str:
    """Return the URL for aggregating instances."""
    return pygen_client_config.create_api_url("/models/instances/aggregate")


def _get_raw_response(item_type: str) -> dict[str, Any]:
    """Helper to get raw data for different item types."""
    if item_type == "product_node":
        return {
            "instanceType": "node",
            "space": "pygen_example",
            "externalId": "product-1",
            "version": 1,
            "lastUpdatedTime": 1234567890000,
            "createdTime": 1234567890000,
            "deletedTime": None,
            "properties": {
                "pygen_example": {
                    "ProductNode/v1": {
                        "active": None,
                        "category": None,
                        "name": "Widget",
                        "price": 19.99,
                        "quantity": 100,
                        "createdDate": "2024-01-01",
                        "description": None,
                        "tags": None,
                        "prices": None,
                        "quantities": None,
                        "updatedTimestamp": None,
                    }
                }
            },
        }
    elif item_type == "category_node":
        return {
            "instanceType": "node",
            "space": "pygen_example",
            "externalId": "category-1",
            "version": 1,
            "lastUpdatedTime": 1234567890000,
            "createdTime": 1234567890000,
            "deletedTime": None,
            "properties": {"pygen_example": {"CategoryNode/v1": {"categoryName": "Electronics"}}},
        }
    elif item_type == "relates_to":
        return {
            "instanceType": "edge",
            "space": "pygen_example",
            "externalId": "edge-1",
            "version": 1,
            "lastUpdatedTime": 1234567890000,
            "createdTime": 1234567890000,
            "startNode": {"space": "pygen_example", "externalId": "start-node"},
            "endNode": {"space": "pygen_example", "externalId": "end-node"},
            "deletedTime": None,
            "properties": {
                "pygen_example": {
                    "RelatesTo/v1": {
                        "relationType": "similar",
                        "createdAt": "2024-01-01T00:00:00.000",
                        "strength": 0.8,
                    }
                }
            },
        }
    else:
        raise ValueError(f"Unknown item type: {item_type}")


class TestExampleAPI:
    API_CLASSES: ClassVar[list[tuple]] = [
        pytest.param("product_node", ProductNode, ProductNodeAPI, id="ProductNode"),
        pytest.param("category_node", CategoryNode, CategoryNodeAPI, id="CategoryNode"),
        pytest.param("relates_to", RelatesTo, RelatesToAPI, id="RelatesTo edge"),
    ]

    @pytest.mark.parametrize("item_type, item_cls, api_cls", API_CLASSES)
    def test_data_class_serialization(
        self,
        item_type: str,
        item_cls: type[Instance],
        api_cls: type[InstanceAPI],
    ) -> None:
        """Test data class parsing and serialization."""
        raw_data = _get_raw_response(item_type)
        item = item_cls.model_validate(raw_data)
        dumped = item.dump(format="instance")
        assert dumped == raw_data
        assert hasattr(item, "as_write")
        writable = item.as_write()
        assert isinstance(writable, InstanceWrite)

    @pytest.mark.parametrize("item_type, item_cls, api_cls", API_CLASSES)
    def test_retrieve(
        self,
        item_type: str,
        item_cls: type[Instance],
        api_cls: type[InstanceAPI],
        example_client: ExampleClient,
        respx_mock: respx.MockRouter,
        retrieve_url: str,
    ) -> None:
        """Test retrieving an item by external ID."""
        raw_data = _get_raw_response(item_type)
        instance_api = getattr(example_client, item_type)
        assert isinstance(instance_api, api_cls)

        respx_mock.post(retrieve_url).respond(json={"items": [raw_data]})
        assert hasattr(instance_api, "retrieve")
        result = instance_api.retrieve(raw_data["externalId"], space="pygen_example")

        assert isinstance(result, item_cls)
        assert result.dump(format="instance") == raw_data

    @pytest.mark.parametrize("item_type, item_cls, api_cls", API_CLASSES)
    def test_list(
        self,
        item_type: str,
        item_cls: type[Instance],
        api_cls: type[InstanceAPI],
        example_client: ExampleClient,
        respx_mock: respx.MockRouter,
        list_url: str,
    ) -> None:
        """Test listing items."""
        raw_data = _get_raw_response(item_type)
        instance_api = getattr(example_client, item_type)
        assert isinstance(instance_api, api_cls)

        respx_mock.post(list_url).respond(json={"items": [raw_data]})
        assert hasattr(instance_api, "list")
        result = instance_api.list()

        assert isinstance(result, UserList)
        assert len(result) == 1
        assert isinstance(result[0], item_cls)
        assert result[0].dump(format="instance") == raw_data

    @pytest.mark.parametrize("item_type, item_cls, api_cls", API_CLASSES)
    def test_iterate(
        self,
        item_type: str,
        item_cls: type[Instance],
        api_cls: type[InstanceAPI],
        example_client: ExampleClient,
        respx_mock: respx.MockRouter,
        list_url: str,
    ) -> None:
        """Test iterating over items with pagination."""
        raw_data = _get_raw_response(item_type)
        instance_api = getattr(example_client, item_type)
        assert isinstance(instance_api, api_cls)

        respx_mock.post(list_url).respond(json={"items": [raw_data], "nextCursor": "cursor123"})
        assert hasattr(instance_api, "iterate")
        page = instance_api.iterate(limit=10)

        assert hasattr(page, "items")
        assert len(page.items) == 1
        assert isinstance(page.items[0], item_cls)
        assert page.items[0].dump(format="instance") == raw_data
        assert page.next_cursor == "cursor123"

    @pytest.mark.parametrize("item_type, item_cls, api_cls", API_CLASSES)
    def test_search(
        self,
        item_type: str,
        item_cls: type[Instance],
        api_cls: type[InstanceAPI],
        example_client: ExampleClient,
        respx_mock: respx.MockRouter,
        search_url: str,
    ) -> None:
        """Test searching for items."""
        raw_data = _get_raw_response(item_type)
        instance_api = getattr(example_client, item_type)
        assert isinstance(instance_api, api_cls)

        respx_mock.post(search_url).respond(json={"items": [raw_data]})
        assert hasattr(instance_api, "search")
        result = instance_api.search()

        assert isinstance(result, UserList)
        assert len(result) == 1
        assert isinstance(result[0], item_cls)
        assert result[0].dump(format="instance") == raw_data

    @pytest.mark.parametrize("item_type, item_cls, api_cls", API_CLASSES)
    def test_aggregate(
        self,
        item_type: str,
        item_cls: type[Instance],
        api_cls: type[InstanceAPI],
        example_client: ExampleClient,
        respx_mock: respx.MockRouter,
        aggregate_url: str,
    ) -> None:
        """Test aggregating items."""
        instance_api = getattr(example_client, item_type)
        assert isinstance(instance_api, api_cls)

        respx_mock.post(aggregate_url).respond(
            json={"items": [{"instanceType": "node", "aggregates": [{"aggregate": "count", "value": 37}]}]}
        )
        assert hasattr(instance_api, "aggregate")
        result = instance_api.aggregate(Count())

        assert isinstance(result, AggregateResponse)
        assert len(result.items) == 1
        aggregates = result.items[0].aggregates
        assert len(aggregates) == 1
        aggregate = aggregates[0]
        assert isinstance(aggregate, AggregatedNumberValue)
        assert aggregate.value == 37
