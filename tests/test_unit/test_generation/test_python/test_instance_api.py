"""Tests for the InstanceAPI class (iterate, list, search methods)."""

import gzip
import json

import pytest
import respx
from httpx import Response

from cognite.pygen._generation.python.instance_api import (
    DebugInfo,
    Instance,
    InstanceAPI,
    InstanceList,
    PropertySort,
    UnitConversion,
    ViewReference,
)
from cognite.pygen._generation.python.instance_api.auth.credentials import Credentials
from cognite.pygen._generation.python.instance_api.config import PygenClientConfig
from cognite.pygen._generation.python.instance_api.http_client import HTTPClient
from cognite.pygen._generation.python.instance_api.models.filters import EqualsFilterData


class MockCredentials(Credentials):
    """Mock credentials for testing."""

    def authorization_header(self) -> tuple[str, str]:
        """Return a mock authorization header."""
        return ("Authorization", "Bearer mock-token")


# Sample view-specific classes for testing
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
def api(http_client: HTTPClient, view_ref: ViewReference) -> InstanceAPI[None, Person, PersonList]:
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
    items: list[dict],
    next_cursor: str | None = None,
    include_typing: bool = False,
    include_debug: bool = False,
) -> dict:
    """Helper to create a list response JSON."""
    response = {"items": items}
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


def make_person_item(external_id: str, name: str, age: int) -> dict:
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
        api: InstanceAPI[None, Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing with no results."""
        respx_mock.post(list_url).respond(json=make_list_response([]))

        result = api.list()

        assert isinstance(result, PersonList)
        assert len(result) == 0

    def test_list_single_page(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing instances that fit in a single page."""
        items = [
            make_person_item("person-1", "Alice", 30),
            make_person_item("person-2", "Bob", 25),
        ]
        respx_mock.post(list_url).respond(json=make_list_response(items))

        result = api.list(limit=10)

        assert isinstance(result, PersonList)
        assert len(result) == 2
        assert result[0].name == "Alice"
        assert result[0].age == 30
        assert result[1].name == "Bob"
        assert result[1].age == 25

    def test_list_with_filter(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing with a filter applied."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        filter_data = {"equals": EqualsFilterData(property=["test", "Person/1", "name"], value="Alice")}
        result = api.list(filter=filter_data)

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
        api: InstanceAPI[None, Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing with sorting."""
        items = [
            make_person_item("person-2", "Bob", 25),
            make_person_item("person-1", "Alice", 30),
        ]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        sort = PropertySort(property=["test", "Person/1", "age"], direction="ascending")
        result = api.list(sort=sort)

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
        api: InstanceAPI[None, Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing with multiple sort criteria."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        sorts = [
            PropertySort(property=["test", "Person/1", "age"], direction="ascending"),
            PropertySort(property=["test", "Person/1", "name"], direction="descending"),
        ]
        api.list(sort=sorts)

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert len(body["sort"]) == 2

    def test_list_with_unit_conversion(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        list_url: str,
    ) -> None:
        """Test listing with unit conversion."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        units = UnitConversion(property=["test", "Person/1", "temperature"], target_unit="temperature:fah")
        api.list(units=units)

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "targetUnits" in body
        assert body["targetUnits"][0]["property"] == ["test", "Person/1", "temperature"]
        assert body["targetUnits"][0]["targetUnit"] == "temperature:fah"


class TestInstanceAPIIterate:
    """Tests for InstanceAPI.iterate() method."""

    def test_iterate_single_page(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        list_url: str,
    ) -> None:
        """Test iterating with a single page of results."""
        items = [make_person_item("person-1", "Alice", 30)]
        respx_mock.post(list_url).respond(json=make_list_response(items))

        pages = list(api.iterate())

        assert len(pages) == 1
        assert len(pages[0].items) == 1
        assert pages[0].next_cursor is None

    def test_iterate_multiple_pages(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        list_url: str,
    ) -> None:
        """Test iterating with multiple pages."""
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

        pages = list(api.iterate(chunk_size=3))

        assert len(pages) == 2
        assert len(pages[0].items) == 3
        assert pages[0].next_cursor == "cursor123"
        assert len(pages[1].items) == 2
        assert pages[1].next_cursor is None

    def test_iterate_with_limit(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        list_url: str,
    ) -> None:
        """Test iterating with a total limit."""
        items = [make_person_item(f"person-{i}", f"Person {i}", 20 + i) for i in range(5)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items[:2]))

        pages = list(api.iterate(limit=2))

        assert len(pages) == 1
        # Verify the request had the correct limit
        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["limit"] == 2

    def test_iterate_with_initial_cursor(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        list_url: str,
    ) -> None:
        """Test iterating starting from a cursor."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items))

        pages = list(api.iterate(cursor="start_cursor"))

        assert len(pages) == 1
        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["cursor"] == "start_cursor"

    def test_iterate_with_debug(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        list_url: str,
    ) -> None:
        """Test iterating with debug information."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(list_url).respond(json=make_list_response(items, include_debug=True))

        pages = list(api.iterate(include_debug=True))

        assert len(pages) == 1
        assert pages[0].debug is not None
        assert pages[0].debug.query_time_ms == 10.5
        assert pages[0].debug.parse_time_ms == 2.3

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["includeDebug"] is True


class TestInstanceAPISearch:
    """Tests for InstanceAPI.search() method."""

    def test_search_simple_query(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        search_url: str,
    ) -> None:
        """Test simple text search."""
        items = [make_person_item("person-1", "Alice Smith", 30)]
        route = respx_mock.post(search_url).respond(json=make_list_response(items))

        result = api.search(query="Alice")

        assert len(result) == 1
        assert result[0].name == "Alice Smith"

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["query"] == "Alice"

    def test_search_with_properties(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        search_url: str,
    ) -> None:
        """Test search with specific properties."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(search_url).respond(json=make_list_response(items))

        result = api.search(query="Alice", properties=["name"])

        assert len(result) == 1
        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["properties"] == ["name"]

    def test_search_with_multiple_properties(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        search_url: str,
    ) -> None:
        """Test search with multiple properties."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(search_url).respond(json=make_list_response(items))

        api.search(query="test", properties=["name", "description"])

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["properties"] == ["name", "description"]

    def test_search_with_filter(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        search_url: str,
    ) -> None:
        """Test search combined with filter."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(search_url).respond(json=make_list_response(items))

        filter_data = {"equals": EqualsFilterData(property=["test", "Person/1", "age"], value=30)}
        result = api.search(query="Alice", filter=filter_data)

        assert len(result) == 1
        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "filter" in body
        assert "query" in body

    def test_search_with_sort(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        search_url: str,
    ) -> None:
        """Test search with sorting."""
        items = [
            make_person_item("person-1", "Alice", 30),
            make_person_item("person-2", "Bob", 25),
        ]
        route = respx_mock.post(search_url).respond(json=make_list_response(items))

        sort = PropertySort(property=["test", "Person/1", "age"], direction="ascending")
        api.search(query="test", sort=sort)

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert "sort" in body

    def test_search_limit_capped_at_1000(
        self,
        respx_mock: respx.MockRouter,
        api: InstanceAPI[None, Person, PersonList],
        search_url: str,
    ) -> None:
        """Test that search limit is capped at 1000."""
        items = [make_person_item("person-1", "Alice", 30)]
        route = respx_mock.post(search_url).respond(json=make_list_response(items))

        api.search(query="test", limit=5000)

        request = route.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["limit"] == 1000


class TestPropertySort:
    """Tests for PropertySort data class."""

    def test_property_sort_basic(self) -> None:
        """Test basic PropertySort creation."""
        sort = PropertySort(property=["space", "view/v1", "name"])

        assert sort.property == ["space", "view/v1", "name"]
        assert sort.direction == "ascending"
        assert sort.nulls_first is None

    def test_property_sort_descending(self) -> None:
        """Test PropertySort with descending direction."""
        sort = PropertySort(property=["space", "view/v1", "name"], direction="descending")

        assert sort.direction == "descending"

    def test_property_sort_nulls_first(self) -> None:
        """Test PropertySort with nulls_first."""
        sort = PropertySort(property=["space", "view/v1", "name"], nulls_first=True)

        assert sort.nulls_first is True

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

    def test_unit_conversion_basic(self) -> None:
        """Test basic UnitConversion creation."""
        unit = UnitConversion(property=["space", "view/v1", "temp"], target_unit="temperature:cel")

        assert unit.property == ["space", "view/v1", "temp"]
        assert unit.target_unit == "temperature:cel"

    def test_unit_conversion_serialization(self) -> None:
        """Test UnitConversion serialization to camelCase."""
        unit = UnitConversion(property=["space", "view/v1", "temp"], target_unit="temperature:cel")

        result = unit.model_dump(by_alias=True)
        assert result == {
            "property": ["space", "view/v1", "temp"],
            "targetUnit": "temperature:cel",
        }


class TestDebugInfo:
    """Tests for DebugInfo data class."""

    def test_debug_info_from_response(self) -> None:
        """Test DebugInfo parsing from API response."""
        data = {
            "requestItemsLimit": 1000,
            "queryTimeMs": 15.5,
            "parseTimeMs": 3.2,
            "serializeTimeMs": 1.8,
        }

        info = DebugInfo.model_validate(data)

        assert info.request_items_limit == 1000
        assert info.query_time_ms == 15.5
        assert info.parse_time_ms == 3.2
        assert info.serialize_time_ms == 1.8

    def test_debug_info_partial(self) -> None:
        """Test DebugInfo with partial data."""
        data = {"queryTimeMs": 10.0}

        info = DebugInfo.model_validate(data)

        assert info.query_time_ms == 10.0
        assert info.request_items_limit is None
        assert info.parse_time_ms is None
