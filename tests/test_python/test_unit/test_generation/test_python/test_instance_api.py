"""Tests for the InstanceAPI class (iterate, list, search, retrieve, aggregate methods)."""

import concurrent.futures
import gzip
import json
from typing import Any

import pytest
import respx
from httpx import Response

from cognite.pygen._generation.python.instance_api import (
    AggregatedNumberValue,
    AggregateResponse,
    Avg,
    Count,
    DebugParameters,
    Instance,
    InstanceAPI,
    InstanceId,
    InstanceList,
    ListResponse,
    Max,
    Min,
    Page,
    PropertySort,
    Sum,
    UnitConversion,
    ViewReference,
)
from cognite.pygen._generation.python.instance_api.auth.credentials import Credentials
from cognite.pygen._generation.python.instance_api.config import PygenClientConfig
from cognite.pygen._generation.python.instance_api.http_client import HTTPClient
from cognite.pygen._generation.python.instance_api.models.filters import EqualsFilterData, Filter
from cognite.pygen._generation.python.instance_api.models.query import AggregationAdapter, UnitReference


class MockCredentials(Credentials):
    """Mock credentials for testing."""

    def authorization_header(self) -> tuple[str, str]:
        """Return a mock authorization header."""
        return "Authorization", "Bearer mock-token"


class Person(Instance):
    _view_id = ViewReference(space="test", external_id="Person", version="1")
    name: str
    age: int


class PersonList(InstanceList[Person]):
    _INSTANCE = Person


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
def view_ref() -> ViewReference:
    """Create a view reference for testing."""
    return ViewReference(space="test", external_id="Person", version="1")


@pytest.fixture
def api(http_client: HTTPClient, view_ref: ViewReference) -> InstanceAPI[Person, PersonList]:
    """Create an InstanceAPI for testing."""
    return InstanceAPI(http_client, view_ref, "node", PersonList)


@pytest.fixture
def list_url(config: PygenClientConfig) -> str:
    """Return the URL for listing instances."""
    return config.create_api_url("/models/instances/list")


@pytest.fixture
def search_url(config: PygenClientConfig) -> str:
    """Return the URL for searching instances."""
    return config.create_api_url("/models/instances/search")


def make_list_response(
    items: list[dict[str, Any]],
    next_cursor: str | None = None,
    include_typing: bool = False,
    include_debug: bool = False,
) -> dict[str, Any]:
    """Helper to create a list response JSON."""
    response: dict[str, Any] = {"items": items}
    if next_cursor:
        response["nextCursor"] = next_cursor
    if include_typing:
        response["typing"] = {"test": {"Person/1": {"name": "Text", "age": "Int32"}}}
    if include_debug:
        response["debug"] = {
            "requestItemsLimit": 1000,
            "queryTimeMs": 10.5,
            "parseTimeMs": 2.3,
            "serializeTimeMs": 1.1,
        }
    return response


def make_person_item(external_id: str, name: str, age: int) -> dict[str, Any]:
    """Helper to create a person instance JSON."""
    return {
        "instanceType": "node",
        "space": "test",
        "externalId": external_id,
        "version": 1,
        "lastUpdatedTime": 1234567890000,
        "createdTime": 1234567890000,
        "properties": {"test": {"Person/1": {"name": name, "age": age}}},
    }


class TestInstanceAPIList:
    """Tests for InstanceAPI.list() method."""

    def test_list_empty_result(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing with no results."""
        respx_mock.post(list_url).respond(json=make_list_response([]))

        result = api._list()

        assert isinstance(result, PersonList)
        assert len(result) == 0

    def test_list_single_page(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing instances that fit in a single page."""
        items = [
            make_person_item("person-1", "Alice", 30),
            make_person_item("person-2", "Bob", 25),
        ]
        respx_mock.post(list_url).respond(json=make_list_response(items))

        result = api._list(limit=10)

        assert isinstance(result, PersonList)
        assert len(result) == 2
        assert result[0].name == "Alice"
        assert result[0].age == 30
        assert result[1].name == "Bob"
        assert result[1].age == 25

    def test_list_with_filter(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing with a filter applied."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        filter_data: Filter = {"equals": EqualsFilterData(property=["test", "Person/1", "name"], value="Alice")}
        result = api._list(filter=filter_data)

        assert len(result) == 1
        assert result[0].name == "Alice"

        # Verify the filter was sent correctly
        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "filter" in body
        assert "equals" in body["filter"]

    def test_list_with_sort(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing with sorting."""
        items = [
            make_person_item("person-2", "Bob", 25),
            make_person_item("person-1", "Alice", 30),
        ]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        sort = PropertySort(property=["test", "Person/1", "age"], direction="ascending")
        result = api._list(sort=sort)

        assert len(result) == 2
        assert result[0].age == 25
        assert result[1].age == 30

        # Verify sort was sent
        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "sort" in body
        assert body["sort"][0]["property"] == ["test", "Person/1", "age"]
        assert body["sort"][0]["direction"] == "ascending"

    def test_list_with_multiple_sorts(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing with multiple sort criteria."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        sorts = [
            PropertySort(property=["test", "Person/1", "age"], direction="ascending"),
            PropertySort(property=["test", "Person/1", "name"], direction="descending"),
        ]
        api._list(sort=sorts)

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert len(body["sort"]) == 2

    def test_list_with_unit_conversion(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing with unit conversion."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        units = UnitConversion(property="temperature", unit=UnitReference(external_id="temperature:fah"))
        api._list(target_units=units)

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        # For list(), target units are in the sources array
        assert "sources" in body
        assert "targetUnits" in body["sources"][0]
        assert body["sources"][0]["targetUnits"][0]["property"] == "temperature"
        assert body["sources"][0]["targetUnits"][0]["unit"]["externalId"] == "temperature:fah"

    def test_list_limit_must_be_positive(
        self,
        api: InstanceAPI[Person, PersonList],
    ) -> None:
        """Test that list raises ValueError for invalid limit."""
        with pytest.raises(ValueError, match=r"Limit must be a positive integer or None for no limit\."):
            api._list(limit=0)


class TestInstanceAPIIterate:
    """Tests for InstanceAPI.iterate() method."""

    def test_iterate_single_page(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        list_url: str,
    ) -> None:
        """Test iterating returns a single page of results."""
        items = [make_person_item("person-1", "Alice", 30)]
        respx_mock.post(list_url).respond(json=make_list_response(items))

        page = api._iterate()

        assert isinstance(page, Page)
        assert len(page.items) == 1
        assert page.next_cursor is None

    def test_iterate_with_pagination(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        list_url: str,
    ) -> None:
        """Test iterating with pagination using cursor."""
        # First page
        page1_items = [make_person_item(f"person-{i}", f"Person {i}", 20 + i) for i in range(3)]
        # Second page
        page2_items = [make_person_item(f"person-{i}", f"Person {i}", 20 + i) for i in range(3, 5)]

        route = respx_mock.post(list_url)

        def respond(request):
            body = json.loads(gzip.decompress(request.content))
            if body.get("cursor") is None:
                return Response(200, json=make_list_response(page1_items, next_cursor="cursor123"))
            else:
                return Response(200, json=make_list_response(page2_items))

        route.side_effect = respond

        # First call returns first page with cursor
        page1 = api._iterate(limit=3)
        assert len(page1.items) == 3
        assert page1.next_cursor == "cursor123"

        # Second call with cursor returns second page
        page2 = api._iterate(cursor=page1.next_cursor, limit=3)
        assert len(page2.items) == 2
        assert page2.next_cursor is None

    def test_iterate_with_limit(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        list_url: str,
    ) -> None:
        """Test iterating with a limit."""
        items = [make_person_item(f"person-{i}", f"Person {i}", 20 + i) for i in range(2)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        page = api._iterate(limit=2)

        assert len(page.items) == 2
        # Verify the request had the correct limit
        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["limit"] == 2

    def test_iterate_with_initial_cursor(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        list_url: str,
    ) -> None:
        """Test iterating starting from a cursor."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        page = api._iterate(cursor="start_cursor")

        assert len(page.items) == 1
        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["cursor"] == "start_cursor"

    def test_iterate_with_debug(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        list_url: str,
    ) -> None:
        """Test iterating with debug information."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items, include_debug=True))

        debug_params = DebugParameters(emit_results=True)
        page = api._iterate(debug=debug_params)

        assert page.debug is not None
        assert page.debug["queryTimeMs"] == 10.5
        assert page.debug["parseTimeMs"] == 2.3

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "debug" in body
        assert body["debug"]["emitResults"] is True

    def test_iterate_limit_must_be_positive(
        self,
        api: InstanceAPI[Person, PersonList],
    ) -> None:
        """Test that iterate raises ValueError for invalid limit."""
        with pytest.raises(ValueError, match="Limit must be between 1 and 1000"):
            api._iterate(limit=0)

    def test_iterate_limit_max_1000(
        self,
        api: InstanceAPI[Person, PersonList],
    ) -> None:
        """Test that iterate raises ValueError for limit > 1000."""
        with pytest.raises(ValueError, match="Limit must be between 1 and 1000"):
            api._iterate(limit=1001)


class TestInstanceAPISearch:
    """Tests for InstanceAPI.search() method."""

    def test_search_simple_query(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        search_url: str,
    ) -> None:
        """Test simple text search."""
        items = [make_person_item("person-1", "Alice Smith", 30)]
        route = respx_mock.post(search_url).respond(json=make_list_response(items))

        result = api._search(query="Alice")

        assert isinstance(result, ListResponse)
        assert len(result.items) == 1
        assert result.items[0].name == "Alice Smith"

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["query"] == "Alice"

    def test_search_with_properties(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        search_url: str,
    ) -> None:
        """Test search with specific properties."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(search_url).respond(json=make_list_response(items))

        result = api._search(query="Alice", properties=["name"])

        assert len(result.items) == 1
        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["properties"] == ["name"]

    def test_search_with_multiple_properties(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        search_url: str,
    ) -> None:
        """Test search with multiple properties."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(search_url).respond(json=make_list_response(items))

        api._search(query="test", properties=["name", "description"])

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["properties"] == ["name", "description"]

    def test_search_with_filter(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        search_url: str,
    ) -> None:
        """Test search combined with filter."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(search_url).respond(json=make_list_response(items))

        filter_data: Filter = {"equals": EqualsFilterData(property=["test", "Person/1", "age"], value=30)}
        result = api._search(query="Alice", filter=filter_data)

        assert len(result.items) == 1
        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "filter" in body
        assert "query" in body

    def test_search_with_sort(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        search_url: str,
    ) -> None:
        """Test search with sorting."""
        items = [
            make_person_item("person-1", "Alice", 30),
            make_person_item("person-2", "Bob", 25),
        ]
        route = respx_mock.post(search_url).respond(json=make_list_response(items))

        sort = PropertySort(property=["test", "Person/1", "age"], direction="ascending")
        api._search(query="test", sort=sort)

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "sort" in body

    def test_search_limit_must_be_positive(
        self,
        api: InstanceAPI[Person, PersonList],
    ) -> None:
        """Test that search raises ValueError for invalid limit."""
        with pytest.raises(ValueError, match="Limit must be between 1 and 1000"):
            api._search(query="test", limit=0)

    def test_search_limit_max_1000(
        self,
        api: InstanceAPI[Person, PersonList],
    ) -> None:
        """Test that search raises ValueError for limit > 1000."""
        with pytest.raises(ValueError, match="Limit must be between 1 and 1000"):
            api._search(query="test", limit=1001)


class TestPropertySort:
    """Tests for PropertySort data class."""

    def test_property_sort_serialization(self) -> None:
        """Test PropertySort serialization to camelCase."""
        sort = PropertySort(property=["space", "view/v1", "name"], nulls_first=True)

        result = sort.model_dump(by_alias=True, exclude_none=True)
        assert result == {
            "property": ["space", "view/v1", "name"],
            "direction": "ascending",
            "nullsFirst": True,
        }

    def test_property_sort_validation_min_length(self) -> None:
        """Test PropertySort validation for minimum property length."""
        with pytest.raises(ValueError):
            PropertySort(property=["single"])


class TestUnitConversion:
    """Tests for UnitConversion data class."""

    def test_unit_conversion_serialization(self) -> None:
        """Test UnitConversion serialization to camelCase."""
        unit = UnitConversion(property="temperature", unit=UnitReference(external_id="temperature:cel"))

        result = unit.model_dump(by_alias=True)
        assert result == {
            "property": "temperature",
            "unit": {"externalId": "temperature:cel"},
        }


class TestDebugParameters:
    """Tests for DebugParameters data class."""

    def test_debug_parameters_serialization(self) -> None:
        """Test DebugParameters serialization to camelCase."""
        params = DebugParameters(emit_results=False, timeout=5000, profile=True)

        result = params.model_dump(by_alias=True, exclude_none=True)
        assert result == {
            "emitResults": False,
            "timeout": 5000,
            "profile": True,
        }


# =============================================================================
# Retrieve Tests
# =============================================================================


@pytest.fixture
def retrieve_url(config: PygenClientConfig) -> str:
    """Return the URL for retrieving instances."""
    return config.create_api_url("/models/instances/byids")


def make_retrieve_response(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Helper to create a retrieve response JSON."""
    return {"items": items}


class TestInstanceAPIRetrieve:
    """Tests for InstanceAPI.retrieve() method."""

    def test_retrieve_single_by_instance_id(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        retrieve_url: str,
    ) -> None:
        """Test retrieving a single instance by InstanceId."""
        items = [make_person_item("person-1", "Alice", 30)]
        respx_mock.post(retrieve_url).respond(json=make_retrieve_response(items))

        instance_id = InstanceId(instance_type="node", space="test", external_id="person-1")
        result = api._retrieve(instance_id)

        assert result is not None
        assert isinstance(result, Person)
        assert result.external_id == "person-1"
        assert result.name == "Alice"

    def test_retrieve_single_by_string(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        retrieve_url: str,
    ) -> None:
        """Test retrieving a single instance by string external_id with space."""
        items = [make_person_item("person-1", "Alice", 30)]
        respx_mock.post(retrieve_url).respond(json=make_retrieve_response(items))

        result = api._retrieve("person-1", space="test")

        assert result is not None
        assert result.external_id == "person-1"

    def test_retrieve_single_by_tuple(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        retrieve_url: str,
    ) -> None:
        """Test retrieving a single instance by (space, external_id) tuple."""
        items = [make_person_item("person-1", "Alice", 30)]
        respx_mock.post(retrieve_url).respond(json=make_retrieve_response(items))

        result = api._retrieve(("test", "person-1"))

        assert result is not None
        assert result.external_id == "person-1"

    def test_retrieve_single_not_found(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        retrieve_url: str,
    ) -> None:
        """Test retrieving a single instance that doesn't exist returns None."""
        respx_mock.post(retrieve_url).respond(json=make_retrieve_response([]))

        result = api._retrieve("non-existent", space="test")

        assert result is None

    def test_retrieve_batch(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        retrieve_url: str,
    ) -> None:
        """Test retrieving multiple instances."""
        items = [
            make_person_item("person-1", "Alice", 30),
            make_person_item("person-2", "Bob", 25),
        ]
        respx_mock.post(retrieve_url).respond(json=make_retrieve_response(items))

        result = api._retrieve(
            [
                InstanceId(instance_type="node", space="test", external_id="person-1"),
                InstanceId(instance_type="node", space="test", external_id="person-2"),
            ]
        )

        assert isinstance(result, PersonList)
        assert len(result) == 2
        assert result[0].name == "Alice"
        assert result[1].name == "Bob"

    def test_retrieve_batch_mixed_types(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        retrieve_url: str,
    ) -> None:
        """Test retrieving with mixed identifier types."""
        items = [
            make_person_item("person-1", "Alice", 30),
            make_person_item("person-2", "Bob", 25),
        ]
        respx_mock.post(retrieve_url).respond(json=make_retrieve_response(items))

        result = api._retrieve(
            [
                "person-1",
                ("test", "person-2"),
            ],
            space="test",
        )

        assert len(result) == 2

    def test_retrieve_empty_list(
        self,
        api: InstanceAPI[Person, PersonList],
    ) -> None:
        """Test retrieving with an empty list returns empty PersonList."""
        result = api._retrieve([])

        assert isinstance(result, PersonList)
        assert len(result) == 0

    def test_retrieve_requires_space_for_string(
        self,
        api: InstanceAPI[Person, PersonList],
    ) -> None:
        """Test that retrieve raises ValueError when space is missing for string id."""
        with pytest.raises(ValueError, match="space parameter is required"):
            api._retrieve("person-1")

    def test_retrieve_request_body_format(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        retrieve_url: str,
    ) -> None:
        """Test the request body format for retrieve."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(retrieve_url).respond(json=make_retrieve_response(items))

        api._retrieve("person-1", space="test", include_typing=True)

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))

        assert body == {
            "instanceType": "node",
            "items": [{"instanceType": "node", "space": "test", "externalId": "person-1"}],
            "sources": [{"source": {"type": "view", "space": "test", "externalId": "Person", "version": "1"}}],
            "includeTyping": True,
        }

    def test_retrieve_with_executor(
        self,
        respx_mock: respx.MockRouter,
        http_client: HTTPClient,
        view_ref: ViewReference,
        retrieve_url: str,
    ) -> None:
        """Test retrieving with a thread pool executor."""
        items = [make_person_item("person-1", "Alice", 30)]
        respx_mock.post(retrieve_url).respond(json=make_retrieve_response(items))

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            api = InstanceAPI[Person, PersonList](http_client, view_ref, "node", PersonList, retrieve_executor=executor)
            result = api._retrieve("person-1", space="test")

        assert result is not None
        assert result.name == "Alice"


# =============================================================================
# Aggregate Tests
# =============================================================================


@pytest.fixture
def aggregate_url(config: PygenClientConfig) -> str:
    """Return the URL for aggregating instances."""
    return config.create_api_url("/models/instances/aggregate")


def make_aggregate_response(
    aggregates: list[dict[str, Any]] | None = None,
    group: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Helper to create an aggregate response JSON."""
    item: dict[str, Any] = {"instanceType": "node", "aggregates": aggregates or []}
    if group is not None:
        item["group"] = group
    return {"items": [item]}


class TestInstanceAPIAggregate:
    """Tests for InstanceAPI.aggregate() method."""

    def test_aggregate_count(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        aggregate_url: str,
    ) -> None:
        """Test count aggregation."""
        response = make_aggregate_response([{"aggregate": "count", "value": 42}])
        respx_mock.post(aggregate_url).respond(json=response)

        result = api._aggregate(Count())  # type: ignore[arg-type]

        assert isinstance(result, AggregateResponse)
        assert len(result.items) == 1
        assert len(result.items[0].aggregates) == 1
        agg = result.items[0].aggregates[0]
        assert agg.aggregate == "count"
        assert isinstance(agg, AggregatedNumberValue)
        assert agg.value == 42

    def test_aggregate_with_aggregation_object(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        aggregate_url: str,
    ) -> None:
        """Test aggregation with Aggregation object."""
        response = make_aggregate_response(aggregates=[{"aggregate": "avg", "property": "age", "value": 27.5}])
        respx_mock.post(aggregate_url).respond(json=response)

        result = api._aggregate(Avg(property="age"))  # type: ignore[arg-type]

        assert len(result.items) == 1
        assert len(result.items[0].aggregates) == 1
        agg = result.items[0].aggregates[0]
        assert agg.aggregate == "avg"
        assert agg.property == "age"
        assert isinstance(agg, AggregatedNumberValue)
        assert agg.value == 27.5

    def test_aggregate_multiple(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        aggregate_url: str,
    ) -> None:
        """Test multiple aggregations."""
        response = make_aggregate_response(
            aggregates=[
                {"aggregate": "min", "property": "age", "value": 20},
                {"aggregate": "max", "property": "age", "value": 50},
                {"aggregate": "avg", "property": "age", "value": 35},
            ]
        )
        respx_mock.post(aggregate_url).respond(json=response)

        result = api._aggregate([Min(property="age"), Max(property="age"), Avg(property="age")])  # type: ignore[list-item]

        assert len(result.items) == 1
        assert len(result.items[0].aggregates) == 3
        values = {
            agg.aggregate: agg.value for agg in result.items[0].aggregates if isinstance(agg, AggregatedNumberValue)
        }
        assert values == {"min": 20, "max": 50, "avg": 35}

    def test_aggregate_with_group_by(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        aggregate_url: str,
    ) -> None:
        """Test aggregation with group_by."""
        response = make_aggregate_response(
            aggregates=[{"aggregate": "count", "property": "externalId", "value": 10}],
            group={"category": "A"},
        )
        respx_mock.post(aggregate_url).respond(json=response)

        result = api._aggregate(Count(property="externalId"), group_by="category")  # type: ignore[arg-type]

        assert len(result.items) == 1
        assert result.items[0].group == {"category": "A"}
        assert len(result.items[0].aggregates) == 1
        agg = result.items[0].aggregates[0]
        assert isinstance(agg, AggregatedNumberValue)
        assert agg.value == 10

    def test_aggregate_with_filter(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        aggregate_url: str,
    ) -> None:
        """Test aggregation with filter."""
        response = make_aggregate_response(aggregates=[{"aggregate": "count", "property": "externalId", "value": 5}])
        route = respx_mock.post(aggregate_url).respond(json=response)

        filter_data: Filter = {"equals": EqualsFilterData(property=["test", "Person/1", "name"], value="Alice")}
        api._aggregate(Count(property="externalId"), filter=filter_data)  # type: ignore[arg-type]

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "filter" in body
        assert "equals" in body["filter"]

    def test_aggregate_request_body_format(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        aggregate_url: str,
    ) -> None:
        """Test the request body format for aggregate."""
        response = make_aggregate_response(aggregates=[{"aggregate": "avg", "property": "age", "value": 27.5}])
        route = respx_mock.post(aggregate_url).respond(json=response)

        api._aggregate(Avg(property="age"), group_by=["category", "status"], query="test", properties=["name"])  # type: ignore[arg-type]

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))

        assert body["instanceType"] == "node"
        assert body["view"] == {"type": "view", "space": "test", "externalId": "Person", "version": "1"}
        assert body["aggregates"] == [{"avg": {"property": "age"}}]
        assert body["groupBy"] == ["category", "status"]
        assert body["query"] == "test"
        assert body["properties"] == ["name"]

    def test_aggregate_multiple_aggregations(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[Person, PersonList],
        aggregate_url: str,
    ) -> None:
        """Test aggregation with multiple aggregation objects."""
        response = make_aggregate_response(
            aggregates=[
                {"aggregate": "sum", "property": "age", "value": 100},
                {"aggregate": "sum", "property": "score", "value": 500},
            ]
        )
        route = respx_mock.post(aggregate_url).respond(json=response)

        api._aggregate([Sum(property="age"), Sum(property="score")])  # type: ignore[list-item]

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert len(body["aggregates"]) == 2
        assert body["aggregates"][0] == {"sum": {"property": "age"}}
        assert body["aggregates"][1] == {"sum": {"property": "score"}}


class TestAggregationClasses:
    """Tests for aggregation data classes."""

    @pytest.mark.parametrize(
        "raw_data",
        [
            pytest.param({"count": {}}, id="Count Aggregation"),
            pytest.param({"avg": {"property": "age"}}, id="Avg Aggregation"),
            pytest.param({"min": {"property": "age"}}, id="Min Aggregation"),
            pytest.param({"max": {"property": "age"}}, id="Max Aggregation"),
            pytest.param({"sum": {"property": "age"}}, id="Sum Aggregation"),
            pytest.param({"histogram": {"property": "age", "interval": 5}}, id="Histogram Aggregation"),
        ],
    )
    def test_roundtrip_serialization(self, raw_data: dict[str, Any]) -> None:
        """Test serialization and deserialization of aggregation classes."""
        class_ = AggregationAdapter.validate_python(raw_data)

        assert AggregationAdapter.dump_python(class_, exclude_unset=True) == raw_data

    @pytest.mark.parametrize(
        "raw_data,error_msg",
        [
            pytest.param({"unknown_agg": {}}, "Unknown aggregate: 'unknown_agg'.", id="Unknown Aggregation"),
            pytest.param(["not", "a", "dict"], "Input should be a valid dictionary", id="Invalid Type"),
            pytest.param({}, "Aggregate data must have exactly one key", id="Empty Dict"),
            pytest.param(
                {"count": {}, "avg": {"property": "age"}},
                "Aggregate data must have exactly one key.",
                id="Multiple Aggregations in One Dictionary",
            ),
        ],
    )
    def test_invalid_aggregation_data(self, raw_data: Any, error_msg: str) -> None:
        """Test that invalid aggregation data raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            AggregationAdapter.validate_python(raw_data)
        assert error_msg in str(exc_info.value)
