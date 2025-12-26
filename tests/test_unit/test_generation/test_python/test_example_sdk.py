"""Tests for the Example SDK (ProductNode, CategoryNode, RelatesTo)."""

import gzip
import json
from datetime import date, datetime
from typing import Any

import pytest
import respx

from cognite.pygen._generation.python.example import (
    CategoryNode,
    CategoryNodeList,
    CategoryNodeWrite,
    ExampleClient,
    ProductNode,
    ProductNodeList,
    ProductNodeWrite,
    RelatesTo,
    RelatesToList,
    RelatesToWrite,
)
from cognite.pygen._generation.python.instance_api import InstanceId
from cognite.pygen._generation.python.instance_api.auth.credentials import Credentials
from cognite.pygen._generation.python.instance_api.config import PygenClientConfig
from cognite.pygen._generation.python.instance_api.http_client import HTTPClient


class MockCredentials(Credentials):
    """Mock credentials for testing."""

    def authorization_header(self) -> tuple[str, str]:
        return "Authorization", "Bearer mock-token"


@pytest.fixture
def config() -> PygenClientConfig:
    """Create a test configuration."""
    return PygenClientConfig(
        cdf_url="https://test.cognitedata.com",
        project="test-project",
        credentials=MockCredentials(),
    )


@pytest.fixture
def http_client(config: PygenClientConfig) -> HTTPClient:
    """Create an HTTP client for testing."""
    return HTTPClient(config)


@pytest.fixture
def list_url(config: PygenClientConfig) -> str:
    """Return the URL for listing instances."""
    return config.create_api_url("/models/instances/list")


@pytest.fixture
def retrieve_url(config: PygenClientConfig) -> str:
    """Return the URL for retrieving instances."""
    return config.create_api_url("/models/instances/byids")


@pytest.fixture
def search_url(config: PygenClientConfig) -> str:
    """Return the URL for searching instances."""
    return config.create_api_url("/models/instances/search")


def make_list_response(items: list[dict[str, Any]], next_cursor: str | None = None) -> dict[str, Any]:
    """Helper to create a list response JSON."""
    response: dict[str, Any] = {"items": items}
    if next_cursor:
        response["nextCursor"] = next_cursor
    return response


def make_product_item(
    external_id: str,
    name: str,
    price: float,
    quantity: int,
    created_date: str = "2024-01-01",
    description: str | None = None,
    active: bool | None = None,
) -> dict[str, Any]:
    """Helper to create a ProductNode instance JSON."""
    props = {
        "name": name,
        "price": price,
        "quantity": quantity,
        "createdDate": created_date,
    }
    if description is not None:
        props["description"] = description
    if active is not None:
        props["active"] = active
    return {
        "instanceType": "node",
        "space": "pygen_example",
        "externalId": external_id,
        "version": 1,
        "lastUpdatedTime": 1234567890000,
        "createdTime": 1234567890000,
        "properties": {"pygen_example": {"ProductNode/v1": props}},
    }


def make_category_item(external_id: str, category_name: str) -> dict[str, Any]:
    """Helper to create a CategoryNode instance JSON."""
    return {
        "instanceType": "node",
        "space": "pygen_example",
        "externalId": external_id,
        "version": 1,
        "lastUpdatedTime": 1234567890000,
        "createdTime": 1234567890000,
        "properties": {"pygen_example": {"CategoryNode/v1": {"categoryName": category_name}}},
    }


def make_relates_to_item(
    external_id: str,
    relation_type: str,
    created_at: str = "2024-01-01T00:00:00.000",
    strength: float | None = None,
) -> dict[str, Any]:
    """Helper to create a RelatesTo edge instance JSON."""
    props: dict[str, Any] = {"relationType": relation_type, "createdAt": created_at}
    if strength is not None:
        props["strength"] = strength
    return {
        "instanceType": "edge",
        "space": "pygen_example",
        "externalId": external_id,
        "version": 1,
        "lastUpdatedTime": 1234567890000,
        "createdTime": 1234567890000,
        "startNode": {"space": "pygen_example", "externalId": "start-node"},
        "endNode": {"space": "pygen_example", "externalId": "end-node"},
        "properties": {"pygen_example": {"RelatesTo/v1": props}},
    }


# =============================================================================
# ProductNode Tests
# =============================================================================


class TestProductNodeDataClass:
    """Tests for ProductNode data classes."""

    def test_product_node_parse_from_api(self) -> None:
        """Test parsing ProductNode from API response format."""
        data = make_product_item("product-1", "Widget", 19.99, 100, description="A widget", active=True)
        product = ProductNode.model_validate(data)

        assert product.external_id == "product-1"
        assert product.name == "Widget"
        assert product.price == 19.99
        assert product.quantity == 100
        assert product.description == "A widget"
        assert product.active is True
        assert product.created_date == date(2024, 1, 1)

    def test_product_node_write_serialization(self) -> None:
        """Test ProductNodeWrite serialization to API format."""
        write = ProductNodeWrite(
            space="pygen_example",
            external_id="product-1",
            name="Widget",
            price=19.99,
            quantity=100,
            created_date=date(2024, 1, 15),
        )
        dumped = write.dump(camel_case=True, format="model")

        assert dumped["space"] == "pygen_example"
        assert dumped["externalId"] == "product-1"
        assert dumped["name"] == "Widget"
        assert dumped["price"] == 19.99
        assert dumped["quantity"] == 100
        assert dumped["createdDate"] == "2024-01-15"

    def test_product_node_as_write(self) -> None:
        """Test converting ProductNode to ProductNodeWrite."""
        data = make_product_item("product-1", "Widget", 19.99, 100)
        product = ProductNode.model_validate(data)
        write = product.as_write()

        assert isinstance(write, ProductNodeWrite)
        assert write.name == product.name
        assert write.price == product.price
        assert write.quantity == product.quantity


class TestProductNodeAPI:
    """Tests for ProductNodeAPI."""

    @pytest.fixture
    def product_api(self, http_client: HTTPClient) -> ProductNodeAPI:
        """Create a ProductNodeAPI for testing."""
        return ProductNodeAPI(http_client)

    def test_list_returns_product_node_list(
        self,
        respx_mock: respx.MockRouter,
        product_api: ProductNodeAPI,
        list_url: str,
    ) -> None:
        """Test that list returns a ProductNodeList."""
        items = [make_product_item("product-1", "Widget", 19.99, 100)]
        respx_mock.post(list_url).respond(json=make_list_response(items))

        result = product_api.list()

        assert isinstance(result, ProductNodeList)
        assert len(result) == 1
        assert result[0].name == "Widget"

    def test_list_with_name_filter(
        self,
        respx_mock: respx.MockRouter,
        product_api: ProductNodeAPI,
        list_url: str,
    ) -> None:
        """Test list with name filter."""
        items = [make_product_item("product-1", "Widget", 19.99, 100)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        product_api.list(name="Widget")

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "filter" in body
        assert body["filter"]["equals"]["property"] == ["pygen_example", "ProductNode/v1", "name"]
        assert body["filter"]["equals"]["value"] == "Widget"

    def test_list_with_price_range(
        self,
        respx_mock: respx.MockRouter,
        product_api: ProductNodeAPI,
        list_url: str,
    ) -> None:
        """Test list with price range filter."""
        items = [make_product_item("product-1", "Widget", 19.99, 100)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        product_api.list(min_price=10.0, max_price=50.0)

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "filter" in body
        assert body["filter"]["range"]["gte"] == 10.0
        assert body["filter"]["range"]["lte"] == 50.0

    def test_list_with_multiple_filters_uses_and(
        self,
        respx_mock: respx.MockRouter,
        product_api: ProductNodeAPI,
        list_url: str,
    ) -> None:
        """Test that multiple filters are combined with AND."""
        items = [make_product_item("product-1", "Widget", 19.99, 100, active=True)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        product_api.list(name="Widget", active=True)

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "filter" in body
        assert "and" in body["filter"]
        assert len(body["filter"]["and"]) == 2

    def test_retrieve_single_returns_product_node(
        self,
        respx_mock: respx.MockRouter,
        product_api: ProductNodeAPI,
        retrieve_url: str,
    ) -> None:
        """Test retrieving a single ProductNode."""
        items = [make_product_item("product-1", "Widget", 19.99, 100)]
        respx_mock.post(retrieve_url).respond(json=make_list_response(items))

        result = product_api.retrieve("product-1", space="pygen_example")

        assert isinstance(result, ProductNode)
        assert result.name == "Widget"

    def test_retrieve_multiple_returns_product_node_list(
        self,
        respx_mock: respx.MockRouter,
        product_api: ProductNodeAPI,
        retrieve_url: str,
    ) -> None:
        """Test retrieving multiple ProductNodes."""
        items = [
            make_product_item("product-1", "Widget", 19.99, 100),
            make_product_item("product-2", "Gadget", 29.99, 50),
        ]
        respx_mock.post(retrieve_url).respond(json=make_list_response(items))

        result = product_api.retrieve(["product-1", "product-2"], space="pygen_example")

        assert isinstance(result, ProductNodeList)
        assert len(result) == 2

    def test_iterate_with_pagination(
        self,
        respx_mock: respx.MockRouter,
        product_api: ProductNodeAPI,
        list_url: str,
    ) -> None:
        """Test iterate returns Page with cursor."""
        items = [make_product_item("product-1", "Widget", 19.99, 100)]
        respx_mock.post(list_url).respond(json=make_list_response(items, next_cursor="cursor123"))

        page = product_api.iterate(limit=10)

        assert len(page.items) == 1
        assert page.next_cursor == "cursor123"


# =============================================================================
# CategoryNode Tests
# =============================================================================


class TestCategoryNodeDataClass:
    """Tests for CategoryNode data classes."""

    def test_category_node_parse_from_api(self) -> None:
        """Test parsing CategoryNode from API response format."""
        data = make_category_item("category-1", "Electronics")
        category = CategoryNode.model_validate(data)

        assert category.external_id == "category-1"
        assert category.category_name == "Electronics"

    def test_category_node_write_serialization(self) -> None:
        """Test CategoryNodeWrite serialization."""
        write = CategoryNodeWrite(
            space="pygen_example",
            external_id="category-1",
            category_name="Electronics",
        )
        dumped = write.dump(camel_case=True, format="model")

        assert dumped["space"] == "pygen_example"
        assert dumped["externalId"] == "category-1"
        assert dumped["categoryName"] == "Electronics"


class TestCategoryNodeAPI:
    """Tests for CategoryNodeAPI."""

    @pytest.fixture
    def category_api(self, http_client: HTTPClient) -> CategoryNodeAPI:
        """Create a CategoryNodeAPI for testing."""
        return CategoryNodeAPI(http_client)

    def test_list_returns_category_node_list(
        self,
        respx_mock: respx.MockRouter,
        category_api: CategoryNodeAPI,
        list_url: str,
    ) -> None:
        """Test that list returns a CategoryNodeList."""
        items = [make_category_item("category-1", "Electronics")]
        respx_mock.post(list_url).respond(json=make_list_response(items))

        result = category_api.list()

        assert isinstance(result, CategoryNodeList)
        assert len(result) == 1
        assert result[0].category_name == "Electronics"

    def test_list_with_category_name_filter(
        self,
        respx_mock: respx.MockRouter,
        category_api: CategoryNodeAPI,
        list_url: str,
    ) -> None:
        """Test list with category_name filter."""
        items = [make_category_item("category-1", "Electronics")]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        category_api.list(category_name="Electronics")

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "filter" in body
        assert body["filter"]["equals"]["value"] == "Electronics"


# =============================================================================
# RelatesTo Tests
# =============================================================================


class TestRelatesToDataClass:
    """Tests for RelatesTo data classes."""

    def test_relates_to_parse_from_api(self) -> None:
        """Test parsing RelatesTo from API response format."""
        data = make_relates_to_item("edge-1", "similar", strength=0.8)
        edge = RelatesTo.model_validate(data)

        assert edge.external_id == "edge-1"
        assert edge.relation_type == "similar"
        assert edge.strength == 0.8
        assert edge.instance_type == "edge"

    def test_relates_to_write_serialization(self) -> None:
        """Test RelatesToWrite serialization."""
        write = RelatesToWrite(
            space="pygen_example",
            external_id="edge-1",
            start_node=InstanceId(instance_type="node", space="pygen_example", external_id="product-1"),
            end_node=("pygen_example", "product-2"),
            relation_type="similar",
            strength=0.8,
            created_at=datetime(2024, 1, 15, 10, 30, 0),
        )
        dumped = write.dump(camel_case=True, format="model")

        assert dumped["instanceType"] == "edge"
        assert dumped["relationType"] == "similar"
        assert dumped["strength"] == 0.8


class TestRelatesToAPI:
    """Tests for RelatesToAPI."""

    @pytest.fixture
    def relates_to_api(self, http_client: HTTPClient) -> RelatesToAPI:
        """Create a RelatesToAPI for testing."""
        return RelatesToAPI(http_client)

    def test_list_returns_relates_to_list(
        self,
        respx_mock: respx.MockRouter,
        relates_to_api: RelatesToAPI,
        list_url: str,
    ) -> None:
        """Test that list returns a RelatesToList."""
        items = [make_relates_to_item("edge-1", "similar", strength=0.8)]
        respx_mock.post(list_url).respond(json=make_list_response(items))

        result = relates_to_api.list()

        assert isinstance(result, RelatesToList)
        assert len(result) == 1
        assert result[0].relation_type == "similar"

    def test_list_with_relation_type_filter(
        self,
        respx_mock: respx.MockRouter,
        relates_to_api: RelatesToAPI,
        list_url: str,
    ) -> None:
        """Test list with relation_type filter."""
        items = [make_relates_to_item("edge-1", "similar")]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        relates_to_api.list(relation_type="similar")

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "filter" in body
        assert body["filter"]["equals"]["value"] == "similar"

    def test_list_with_strength_range(
        self,
        respx_mock: respx.MockRouter,
        relates_to_api: RelatesToAPI,
        list_url: str,
    ) -> None:
        """Test list with strength range filter."""
        items = [make_relates_to_item("edge-1", "similar", strength=0.8)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        relates_to_api.list(min_strength=0.5, max_strength=1.0)

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "filter" in body
        assert body["filter"]["range"]["gte"] == 0.5
        assert body["filter"]["range"]["lte"] == 1.0


# =============================================================================
# ExampleClient Tests
# =============================================================================


class TestExampleClient:
    """Tests for ExampleClient."""

    def test_client_has_view_specific_apis(self, config: PygenClientConfig) -> None:
        """Test that ExampleClient has all view-specific APIs."""
        with ExampleClient(config) as client:
            assert hasattr(client, "product_node")
            assert hasattr(client, "category_node")
            assert hasattr(client, "relates_to")
            assert isinstance(client.product_node, ProductNodeAPI)
            assert isinstance(client.category_node, CategoryNodeAPI)
            assert isinstance(client.relates_to, RelatesToAPI)

    def test_client_context_manager(self, config: PygenClientConfig) -> None:
        """Test that ExampleClient works as a context manager."""
        with ExampleClient(config) as client:
            assert client is not None

    def test_client_list_products(
        self,
        respx_mock: respx.MockRouter,
        config: PygenClientConfig,
        list_url: str,
    ) -> None:
        """Test listing products through ExampleClient."""
        items = [make_product_item("product-1", "Widget", 19.99, 100)]
        respx_mock.post(list_url).respond(json=make_list_response(items))

        with ExampleClient(config) as client:
            products = client.product_node.list()

        assert len(products) == 1
        assert products[0].name == "Widget"
