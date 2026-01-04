import pytest

from cognite.pygen._client.models import ViewReference
from cognite.pygen._generator.python import PythonAPIGenerator, PythonDataClassGenerator, PythonPackageGenerator
from cognite.pygen._pygen_model import (
    APIClassFile,
    DataClass,
    DataClassFile,
    Field,
    FilterClass,
    ListDataClass,
    PygenSDKModel,
)


@pytest.fixture(scope="session")
def data_class_file() -> DataClassFile:
    return DataClassFile(
        filename="example.py",
        view_id=ViewReference(
            space="example_space",
            external_id="example_view",
            version="v1",
        ),
        instance_type="node",
        read=DataClass(
            name="ExampleView",
            fields=[
                Field(
                    cdf_prop_id="prop1",
                    name="property_one",
                    type_hint="str | None",
                    filter_name="TextFilter",
                    description="The first property.",
                    default_value="None",
                    dtype="str",
                ),
                Field(
                    cdf_prop_id="prop2",
                    name="property_two",
                    type_hint="int",
                    filter_name="IntegerFilter",
                    description="The second property.",
                    dtype="int",
                ),
            ],
            display_name="Example View",
            description="An example view for testing.",
        ),
        read_list=ListDataClass(
            name="ExampleViewList",
        ),
        filter=FilterClass(
            name="ExampleViewFilter",
        ),
        write=DataClass(
            name="ExampleViewWrite",
            fields=[
                Field(
                    cdf_prop_id="prop1",
                    name="property_one",
                    type_hint="str | None",
                    filter_name=None,
                    description="The first property.",
                    default_value="None",
                    dtype="str",
                ),
                Field(
                    cdf_prop_id="prop2",
                    name="property_two",
                    type_hint="int",
                    filter_name=None,
                    description="The second property.",
                    dtype="int",
                ),
            ],
            display_name="Example View",
            description="An example write view for testing.",
        ),
    )


EXPECTED_IMPORT_STATEMENTS = """from typing import ClassVar, Literal

from pydantic import Field

from cognite.pygen._python.instance_api.models._references import ViewReference
from cognite.pygen._python.instance_api.models.dtype_filters import (
    FilterContainer,
    IntegerFilter,
    TextFilter,
)
from cognite.pygen._python.instance_api.models.instance import (
    Instance,
    InstanceList,
    InstanceWrite,
)"""

EXPECTED_WRITE_CLASS_CODE = '''class ExampleViewWrite(InstanceWrite):
    """Write class for Example View instances."""
    _view_id: ClassVar[ViewReference] = ViewReference(
        space="example_space", external_id="example_view", version="v1"
    )
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    property_one: str | None = Field(default=None, alias="prop1")
    property_two: int = Field(alias="prop2")
'''

EXPECTED_READ_CLASS_CODE = '''class ExampleView(Instance):
    """Read class for Example View instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(
        space="example_space", external_id="example_view", version="v1"
    )
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    property_one: str | None = Field(default=None, alias="prop1")
    property_two: int = Field(alias="prop2")

    def as_write(self) -> ExampleViewWrite:
        """Convert to write representation."""
        return ExampleViewWrite.model_validate(self.model_dump(by_alias=True))
'''

EXPECTED_READ_LIST_CLASS_CODE = '''class ExampleViewList(InstanceList[ExampleView]):
    """List of Example View instances."""
    _INSTANCE: ClassVar[type[ExampleView]] = ExampleView
'''

EXPECTED_FILTER_CLASS_CODE = """class ExampleViewFilter(FilterContainer):
    def __init__(self, operator: Literal["and", "or"] = "and") -> None:
        view_id = ExampleView._view_id
        self.property_one = TextFilter(view_id, "prop1", operator)
        self.property_two = IntegerFilter(view_id, "prop2", operator)
        super().__init__(
            data_type_filters=[
                self.property_one,
                self.property_two,
            ],
            operator=operator,
            instance_type="node",
        )
"""


@pytest.fixture(scope="session")
def data_class_generator(data_class_file: DataClassFile) -> PythonDataClassGenerator:
    return PythonDataClassGenerator(data_class_file, max_line_length=120)


class TestPythonDataClassGenerator:
    def test_create_import_statements(self, data_class_generator: PythonDataClassGenerator) -> None:
        import_statements = data_class_generator.create_import_statements()
        assert import_statements.strip() == EXPECTED_IMPORT_STATEMENTS.strip()

    def test_generate_write_class(self, data_class_generator: PythonDataClassGenerator) -> None:
        write_class_code = data_class_generator.generate_write_class()
        assert write_class_code.strip() == EXPECTED_WRITE_CLASS_CODE.strip()

    def test_generate_read_class(self, data_class_generator: PythonDataClassGenerator) -> None:
        read_class_code = data_class_generator.generate_read_class()
        assert read_class_code.strip() == EXPECTED_READ_CLASS_CODE.strip()

    def test_generate_read_list_class(self, data_class_generator: PythonDataClassGenerator) -> None:
        read_list_class_code = data_class_generator.generate_read_list_class()
        assert read_list_class_code.strip() == EXPECTED_READ_LIST_CLASS_CODE.strip()

    def test_generate_filter_class(self, data_class_generator: PythonDataClassGenerator) -> None:
        filter_class_code = data_class_generator.generate_filter_class()
        assert filter_class_code.strip() == EXPECTED_FILTER_CLASS_CODE.strip()


# ============================================================================
# API Class Generator Tests
# ============================================================================


@pytest.fixture(scope="session")
def api_class_file(data_class_file: DataClassFile) -> APIClassFile:
    return APIClassFile(
        filename="_example_view_api.py",
        name="ExampleViewAPI",
        client_attribute_name="example_view",
        data_class=data_class_file,
    )


@pytest.fixture(scope="session")
def api_class_generator(api_class_file: APIClassFile) -> PythonAPIGenerator:
    return PythonAPIGenerator(api_class_file, top_level="example_sdk_python", max_line_length=120)


EXPECTED_API_IMPORT_STATEMENTS = """from collections.abc import Sequence
from typing import Literal, overload

from cognite.pygen._python.instance_api._api import InstanceAPI
from cognite.pygen._python.instance_api.http_client import HTTPClient
from cognite.pygen._python.instance_api.models import (
    Aggregation,
    InstanceId,
    PropertySort,
    ViewReference,
)
from cognite.pygen._python.instance_api.models.responses import (
    AggregateResponse,
    Page,
)

from example_sdk_python.data_classes import (
    ExampleView,
    ExampleViewFilter,
    ExampleViewList,
)"""

EXPECTED_API_CLASS_WITH_INIT = '''
def _create_property_ref(view_ref: ViewReference, property_name: str) -> list[str]:
    """Create a property reference for filtering."""
    return [view_ref.space, f"{view_ref.external_id}/{view_ref.version}", property_name]


class ExampleViewAPI(InstanceAPI[ExampleView, ExampleViewList]):
    """API for ExampleView instances with type-safe filter methods."""

    def __init__(self, http_client: HTTPClient) -> None:
        view_ref = ViewReference(
            space="example_space", external_id="example_view", version="v1"
        )
        super().__init__(http_client, view_ref, "node", ExampleViewList)'''

EXPECTED_RETRIEVE_METHOD = '''
    @overload
    def retrieve(
        self,
        id: str | InstanceId | tuple[str, str],
        space: str | None = None,
    ) -> ExampleView | None: ...

    @overload
    def retrieve(
        self,
        id: list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> ExampleViewList: ...

    def retrieve(
        self,
        id: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> ExampleView | ExampleViewList | None:
        """Retrieve ExampleView instances by ID.

        Args:
            id: Instance identifier(s). Can be a string, InstanceId, tuple, or list of these.
            space: Default space to use when id is a string.

        Returns:
            For single id: The ExampleView if found, None otherwise.
            For list of ids: A ExampleViewList of found instances.
        """
        return self._retrieve(id, space)'''

EXPECTED_AGGREGATE_METHOD = '''
    def aggregate(
        self,
        aggregate: Aggregation | Sequence[Aggregation],
        group_by: str | Sequence[str] | None = None,
        property_one: str | list[str] | None = None,
        property_one_prefix: str | None = None,
        min_property_two: int | None = None,
        max_property_two: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
    ) -> AggregateResponse:
        """Aggregate instances.

        Args:
            aggregate: Aggregation(s) to perform.
            group_by: Property or properties to group by.
            property_one: Filter by exact property one or list of values.
            property_one_prefix: Filter by property one prefix.
            min_property_two: Minimum property two (inclusive).
            max_property_two: Maximum property two (inclusive).
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.

        Returns:
            AggregateResponse with aggregated values.
        """
        filter_ = ExampleViewFilter("and")
        filter_.property_one.equals_or_in(property_one)
        filter_.property_one.prefix(property_one_prefix)
        filter_.property_two.greater_than_or_equals(min_property_two)
        filter_.property_two.less_than_or_equals(max_property_two)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._aggregate(aggregate=aggregate, group_by=group_by, filter=filter_.as_filter())'''

EXPECTED_SEARCH_METHOD = '''
    def search(
        self,
        query: str | None = None,
        properties: str | Sequence[str] | None = None,
        property_one: str | list[str] | None = None,
        property_one_prefix: str | None = None,
        min_property_two: int | None = None,
        max_property_two: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = 25,
    ) -> ExampleViewList:
        """Search instances using full-text search.

        Args:
            query: The search query string.
            properties: Properties to search in. If None, searches all text properties.
            property_one: Filter by exact property one or list of values.
            property_one_prefix: Filter by property one prefix.
            min_property_two: Minimum property two (inclusive).
            max_property_two: Maximum property two (inclusive).
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            limit: Maximum number of results.

        Returns:
            A ExampleViewList with matching instances.
        """
        filter_ = ExampleViewFilter("and")
        filter_.property_one.equals_or_in(property_one)
        filter_.property_one.prefix(property_one_prefix)
        filter_.property_two.greater_than_or_equals(min_property_two)
        filter_.property_two.less_than_or_equals(max_property_two)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._search(query=query, properties=properties, limit=limit, filter=filter_.as_filter()).items'''

EXPECTED_ITERATE_METHOD = '''
    def iterate(
        self,
        property_one: str | list[str] | None = None,
        property_one_prefix: str | None = None,
        min_property_two: int | None = None,
        max_property_two: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        cursor: str | None = None,
        limit: int = 25,
    ) -> Page[ExampleViewList]:
        """Iterate over instances with pagination.

        Args:
            property_one: Filter by exact property one or list of values.
            property_one_prefix: Filter by property one prefix.
            min_property_two: Minimum property two (inclusive).
            max_property_two: Maximum property two (inclusive).
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            cursor: Pagination cursor from a previous page.
            limit: Maximum number of results per page (1-1000).

        Returns:
            A Page containing items and optional next cursor.
        """
        filter_ = ExampleViewFilter("and")
        filter_.property_one.equals_or_in(property_one)
        filter_.property_one.prefix(property_one_prefix)
        filter_.property_two.greater_than_or_equals(min_property_two)
        filter_.property_two.less_than_or_equals(max_property_two)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._iterate(cursor=cursor, limit=limit, filter=filter_.as_filter())'''

EXPECTED_LIST_METHOD = '''
    def list(
        self,
        property_one: str | list[str] | None = None,
        property_one_prefix: str | None = None,
        min_property_two: int | None = None,
        max_property_two: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        sort_by: str | None = None,
        sort_direction: Literal["ascending", "descending"] = "ascending",
        limit: int | None = 25,
    ) -> ExampleViewList:
        """List instances with type-safe filtering.

        Args:
            property_one: Filter by exact property one or list of values.
            property_one_prefix: Filter by property one prefix.
            min_property_two: Minimum property two (inclusive).
            max_property_two: Maximum property two (inclusive).
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            sort_by: Property name to sort by.
            sort_direction: Sort direction.
            limit: Maximum number of results. None for no limit.

        Returns:
            A ExampleViewList of matching instances.
        """
        filter_ = ExampleViewFilter("and")
        filter_.property_one.equals_or_in(property_one)
        filter_.property_one.prefix(property_one_prefix)
        filter_.property_two.greater_than_or_equals(min_property_two)
        filter_.property_two.less_than_or_equals(max_property_two)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        sort = None
        if sort_by is not None:
            prop_ref = _create_property_ref(self._view_ref, sort_by)
            sort = PropertySort(property=prop_ref, direction=sort_direction)

        return self._list(limit=limit, filter=filter_.as_filter(), sort=sort)'''


class TestPythonAPIGenerator:
    def test_create_import_statements(self, api_class_generator: PythonAPIGenerator) -> None:
        import_statements = api_class_generator.create_import_statements()
        assert import_statements.strip() == EXPECTED_API_IMPORT_STATEMENTS.strip()

    def test_create_api_class_with_init(self, api_class_generator: PythonAPIGenerator) -> None:
        api_class_code = api_class_generator.create_api_class_with_init()
        assert api_class_code.strip() == EXPECTED_API_CLASS_WITH_INIT.strip()

    def test_create_retrieve_method(self, api_class_generator: PythonAPIGenerator) -> None:
        retrieve_method = api_class_generator.create_retrieve_method()
        assert retrieve_method.strip() == EXPECTED_RETRIEVE_METHOD.strip()

    def test_create_aggregate_method(self, api_class_generator: PythonAPIGenerator) -> None:
        aggregate_method = api_class_generator.create_aggregate_method()
        assert aggregate_method.strip() == EXPECTED_AGGREGATE_METHOD.strip()

    def test_create_search_method(self, api_class_generator: PythonAPIGenerator) -> None:
        search_method = api_class_generator.create_search_method()
        assert search_method.strip() == EXPECTED_SEARCH_METHOD.strip()

    def test_create_iterate_method(self, api_class_generator: PythonAPIGenerator) -> None:
        iterate_method = api_class_generator.create_iterate_method()
        assert iterate_method.strip() == EXPECTED_ITERATE_METHOD.strip()

    def test_create_list_method(self, api_class_generator: PythonAPIGenerator) -> None:
        list_method = api_class_generator.create_list_method()
        assert list_method.strip() == EXPECTED_LIST_METHOD.strip()


# ============================================================================
# Package Generator Tests
# ============================================================================


@pytest.fixture(scope="session")
def pygen_sdk_model(data_class_file: DataClassFile, api_class_file: APIClassFile) -> PygenSDKModel:
    """Create a PygenSDKModel with a single view for testing."""
    return PygenSDKModel(
        data_classes=[data_class_file],
        api_classes=[api_class_file],
    )


@pytest.fixture(scope="session")
def package_generator(pygen_sdk_model: PygenSDKModel) -> PythonPackageGenerator:
    return PythonPackageGenerator(pygen_sdk_model, "cognite.pygen._python", "MyClient")


EXPECTED_DATA_CLASS_INIT = '''"""Data classes for the generated SDK.

This module exports all data classes including read, write, list, and filter classes.
"""

from .example import (
    ExampleView,
    ExampleViewFilter,
    ExampleViewList,
    ExampleViewWrite,
)

__all__ = [
    "ExampleView",
    "ExampleViewFilter",
    "ExampleViewList",
    "ExampleViewWrite",
]'''

EXPECTED_API_INIT = '''"""API classes for the generated SDK.

This module exports all view-specific API classes.
"""

from ._example_view_api import ExampleViewAPI

__all__ = [
    "ExampleViewAPI",
]'''

EXPECTED_CLIENT_CODE = '''"""Client for the generated SDK.

This module contains the MyClient that composes view-specific APIs.
"""

from cognite.pygen._python.instance_api._client import InstanceClient
from cognite.pygen._python.instance_api.config import PygenClientConfig

from ._api import ExampleViewAPI


class MyClient(InstanceClient):
    """Generated client for interacting with the data model.

    This client provides access to the following views:
    - example_view: ExampleViewAPI
    """

    def __init__(self, config: PygenClientConfig) -> None:
        """Initialize the client.

        Args:
            config: Configuration for the client including URL, project, and credentials.
        """
        super().__init__(config)

        # Initialize view-specific APIs
        self.example_view = ExampleViewAPI(self._http_client)'''

EXPECTED_PACKAGE_INIT = '''"""Generated SDK package.

This package provides the MyClient for interacting with the data model.
"""

from ._client import MyClient

__all__ = ["MyClient"]
'''


class TestPythonPackageGenerator:
    def test_create_data_class_init(self, package_generator: PythonPackageGenerator) -> None:
        data_class_init = package_generator.create_data_class_init()
        assert data_class_init.strip() == EXPECTED_DATA_CLASS_INIT.strip()

    def test_create_api_init(self, package_generator: PythonPackageGenerator) -> None:
        api_init = package_generator.create_api_init()
        assert api_init.strip() == EXPECTED_API_INIT.strip()

    def test_create_client(self, package_generator: PythonPackageGenerator) -> None:
        client_code = package_generator.create_client()
        assert client_code.strip() == EXPECTED_CLIENT_CODE.strip()

    def test_create_package_init(self, package_generator: PythonPackageGenerator) -> None:
        package_init = package_generator.create_package_init()
        assert package_init.strip() == EXPECTED_PACKAGE_INIT.strip()
