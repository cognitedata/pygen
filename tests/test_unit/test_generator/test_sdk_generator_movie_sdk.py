from __future__ import annotations

import platform
from typing import cast
from unittest.mock import MagicMock

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import ViewProperty

from cognite.pygen._core.data_classes import (
    ViewSpaceExternalId,
)
from cognite.pygen._core.data_class.data_class import NodeDataClass
from cognite.pygen._core.data_class.fields import Field, PrimitiveField, PrimitiveListField, EdgeOneToOne, EdgeOneToMany
from cognite.pygen._core.data_class.filter_method import (
    FilterParameter,
    FilterCondition,
    FilterConditionOnetoOneEdge,
    FilterMethod,
)
from cognite.pygen._core.generators import APIGenerator, MultiAPIGenerator, SDKGenerator
from cognite.pygen._generator import CodeFormatter
from cognite.pygen.config import PygenConfig
from tests.constants import IS_PYDANTIC_V1, IS_PYDANTIC_V2, MovieSDKFiles


@pytest.fixture
def top_level_package() -> str:
    if IS_PYDANTIC_V1:
        return "movie_domain_pydantic_v1.client"
    else:
        return "movie_domain.client"


@pytest.fixture
def client_name() -> str:
    return "MovieClient"


@pytest.fixture
def sdk_generator(movie_model, top_level_package, client_name) -> SDKGenerator:
    return SDKGenerator(top_level_package, client_name, movie_model)


@pytest.fixture
def multi_api_generator(movie_model, top_level_package, pygen_config: PygenConfig) -> MultiAPIGenerator:
    return MultiAPIGenerator(top_level_package, "MovieClient", movie_model.views, config=pygen_config)


def create_fields_test_cases():
    prop = {
        "container": {"space": "IntegrationTestsImmutable", "externalId": "Person"},
        "containerPropertyIdentifier": "name",
        "type": {"list": False, "collation": "ucs_basic", "type": "text"},
        "nullable": False,
        "autoIncrement": False,
        "source": None,
        "defaultValue": None,
        "name": "name",
        "description": None,
    }
    prop = ViewProperty.load(prop)
    #
    yield pytest.param(
        "name",
        prop,
        {},
        "Person",
        PrimitiveField(
            name="name",
            prop_name="name",
            doc_name="name",
            type_="str",
            prop=cast(dm.MappedProperty, prop),
            is_nullable=False,
            default=None,
            pydantic_field="Field",
        ),
        "Optional[str] = None",
        "str",
        id="String property",
    )
    prop = {
        "type": {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
        "source": {"space": "IntegrationTestsImmutable", "externalId": "Role", "version": "2", "type": "view"},
        "name": "roles",
        "description": None,
        "edgeSource": None,
        "direction": "outwards",
    }
    prop = ViewProperty.load(prop)
    data_class = NodeDataClass(
        read_name="Role",
        write_name="RoleApply",
        read_list_name="RoleList",
        write_list_name="RoleListApply",
        doc_name="role",
        doc_list_name="roles",
        variable="role",
        variable_list="roles",
        file_name="_roles",
        view_id=ViewSpaceExternalId("IntegrationTestsImmutable", "Role"),
        view_version="2",
        view_name="Role",
        query_class_name="RoleQueryAPI",
        query_file_name="role_query",
    )
    config = PygenConfig()
    data_class_by_view_id = {ViewSpaceExternalId("IntegrationTestsImmutable", "Role"): data_class}
    yield pytest.param(
        "roles",
        prop,
        data_class_by_view_id,
        "Person",
        EdgeOneToMany(
            name="roles",
            prop_name="roles",
            doc_name="role",
            prop=cast(dm.SingleHopConnectionDefinition, prop),
            data_class=data_class,
            variable="role",
            pydantic_field="Field",
            edge_api_attribute="roles_edge",
            edge_api_class="PersonRolesAPI",
            edge_api_file_name="person_roles",
            _list_method=FilterMethod.from_fields([], config.filtering, is_edge_class=True),
        ),
        "Union[list[Role], list[str], None] = Field(default=None, repr=False)",
        "Union[list[RoleApply], list[str], None] = Field(default=None, repr=False)",
        id="List of edges",
    )
    prop = {
        "container": {"space": "IntegrationTestsImmutable", "externalId": "Command_Config"},
        "containerPropertyIdentifier": "configs",
        "type": {"list": True, "collation": "ucs_basic", "type": "text"},
        "nullable": False,
        "autoIncrement": False,
        "source": None,
        "defaultValue": None,
        "name": "configs",
        "description": None,
    }
    prop = ViewProperty.load(prop)
    yield pytest.param(
        "configs",
        prop,
        {},
        "Command_Config",
        PrimitiveListField(
            name="configs",
            prop_name="configs",
            prop=cast(dm.MappedProperty, prop),
            type_="str",
            is_nullable=False,
            pydantic_field="Field",
            doc_name="config",
        ),
        "Optional[list[str]] = None",
        "list[str]",
        id="List of strings",
    )

    prop = {
        "container": {"space": "IntegrationTestsImmutable", "externalId": "Role"},
        "containerPropertyIdentifier": "person",
        "type": {
            "container": None,
            "type": "direct",
            "source": {"space": "IntegrationTestsImmutable", "externalId": "Person", "version": "2"},
        },
        "nullable": True,
        "autoIncrement": False,
        "defaultValue": None,
        "name": "person",
        "description": None,
    }
    data_class = NodeDataClass(
        read_name="Person",
        write_name="PersonApply",
        read_list_name="PersonList",
        write_list_name="PersonListApply",
        doc_name="person",
        doc_list_name="persons",
        variable="person",
        file_name="_persons",
        view_id=ViewSpaceExternalId("IntegrationTestsImmutable", "Person"),
        view_version="2",
        variable_list="persons",
        view_name="Person",
        query_class_name="PersonQueryAPI",
        query_file_name="person_query",
    )
    data_class_by_view_id = {ViewSpaceExternalId("IntegrationTestsImmutable", "Person"): data_class}

    prop = ViewProperty.load(prop)
    yield pytest.param(
        "person",
        prop,
        data_class_by_view_id,
        "Person",
        EdgeOneToOne(
            name="person",
            prop_name="person",
            doc_name="person",
            prop=cast(dm.MappedProperty, prop),
            data_class=data_class,
            pydantic_field="Field",
        ),
        "Union[Person, str, dm.NodeId, None] = Field(None, repr=False)",
        "Union[PersonApply, str, dm.NodeId, None] = Field(None, repr=False)",
        id="Edge to another view",
    )

    prop = {
        "container": {"space": "IntegrationTestsImmutable", "externalId": "Role"},
        "containerPropertyIdentifier": "wonOscar",
        "type": {"list": False, "type": "boolean"},
        "nullable": True,
        "autoIncrement": False,
        "source": None,
        "defaultValue": None,
        "name": "wonOscar",
        "description": None,
    }
    prop = ViewProperty.load(prop)

    yield pytest.param(
        "wonOscar",
        prop,
        {},
        "Person",
        PrimitiveField(
            name="won_oscar",
            prop_name="wonOscar",
            doc_name="won oscar",
            prop=cast(dm.MappedProperty, prop),
            is_nullable=True,
            default=None,
            type_="bool",
            pydantic_field="Field",
        ),
        'Optional[bool] = Field(None, alias="wonOscar")',
        'Optional[bool] = Field(None, alias="wonOscar")',
        id="Boolean property with pascal name",
    )


@pytest.mark.parametrize(
    "prop_name, property_, data_class_by_view_id, view_name, expected, "
    "expected_read_type_hint, expected_write_type_hint",
    list(create_fields_test_cases()),
)
def test_fields_from_property(
    prop_name: str,
    property_: dm.MappedProperty | dm.ConnectionDefinition,
    data_class_by_view_id: dict[ViewSpaceExternalId, NodeDataClass],
    view_name: str,
    expected: Field,
    expected_read_type_hint: str,
    expected_write_type_hint: str,
    pygen_config: PygenConfig,
):
    # Act
    actual = Field.from_property(
        prop_name, property_, data_class_by_view_id, pygen_config, view_name, dm.ViewId("a", "b", "c")
    )

    # Assert
    assert actual == expected
    assert actual.as_read_type_hint() == expected_read_type_hint
    assert actual.as_write_type_hint() == expected_write_type_hint


@pytest.fixture()
def person_api_generator(multi_api_generator: MultiAPIGenerator, person_view: dm.View) -> APIGenerator:
    api_generator = next(
        (api for api in multi_api_generator.sub_apis if api.view_identifier == ViewSpaceExternalId.from_(person_view)),
        None,
    )
    assert api_generator is not None, "Could not find API generator for actor view"
    return api_generator


@pytest.fixture()
def actor_api_generator(multi_api_generator: MultiAPIGenerator, actor_view: dm.View) -> APIGenerator:
    api_generator = next(
        (api for api in multi_api_generator.sub_apis if api.view_identifier == ViewSpaceExternalId.from_(actor_view)),
        None,
    )
    assert api_generator is not None, "Could not find API generator for actor view"
    return api_generator


def test_generate_data_class_file_persons(
    person_api_generator: APIGenerator, pygen_config: PygenConfig, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.persons_data.read_text()

    # Act
    actual = person_api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_view_data_class_actors(
    actor_api_generator: APIGenerator, pygen_config: PygenConfig, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.actors_data.read_text()

    # Act
    actual = actor_api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_view_api_classes_actors(
    actor_api_generator: APIGenerator, top_level_package: str, client_name: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.actors_api.read_text()

    # Act
    actual = actor_api_generator.generate_api_file(top_level_package, client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_query_api_actors(
    actor_api_generator: APIGenerator, top_level_package: str, client_name: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.actor_query_api.read_text()

    # Act
    actual = actor_api_generator.generate_api_query_file(top_level_package, client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_actor_movie_edge_api(
    actor_api_generator: APIGenerator, top_level_package: str, client_name: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.actor_movies_api.read_text()

    # Act
    _, actual = next(actor_api_generator.generate_edge_api_files(top_level_package, client_name))
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_view_api_classes_persons(
    person_api_generator: APIGenerator, top_level_package: str, client_name: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.persons_api.read_text()

    # Act
    actual = person_api_generator.generate_api_file(top_level_package, client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_data_class_init_file(multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = MovieSDKFiles.data_init.read_text()

    # Act
    actual = multi_api_generator.generate_data_classes_init_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


@pytest.mark.skipif(
    not platform.platform().startswith("Windows"),
    reason="There is currently some strange problem with the diff on non-windows",
)
def test_create_api_client(sdk_generator: SDKGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = MovieSDKFiles.client.read_text()

    # Act
    actual = sdk_generator._generate_api_client_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_api_core_file(multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = MovieSDKFiles.core_api.read_text()

    # Act
    actual = multi_api_generator.generate_api_core_file()

    # Assert
    assert actual == expected


def test_create_list_method(person_view: dm.View, pygen_config: PygenConfig) -> None:
    # Arrange
    data_class = NodeDataClass.from_view(person_view, pygen_config.naming.data_class)

    data_class.update_fields(
        person_view.properties,
        {ViewSpaceExternalId(space="IntegrationTestsImmutable", external_id="Role"): MagicMock(spec=NodeDataClass)},
        config=pygen_config,
    )
    parameters = [
        FilterParameter("min_birth_year", "int", description="The minimum value of the birth year to filter on."),
        FilterParameter("max_birth_year", "int", description="The maximum value of the birth year to filter on."),
        FilterParameter("name", "str | list[str]", description="The name to filter on."),
        FilterParameter("name_prefix", "str", description="The prefix of the name to filter on."),
        FilterParameter("external_id_prefix", "str", description="The prefix of the external ID to filter on."),
        FilterParameter("space", "str | list[str]", description="The space to filter on."),
    ]
    expected = FilterMethod(
        parameters=parameters,
        filters=[
            FilterCondition(
                dm.filters.Range, "birthYear", dict(gte=parameters[0], lte=parameters[1]), is_edge_class=False
            ),
            FilterCondition(dm.filters.Equals, "name", dict(value=parameters[2]), is_edge_class=False),
            FilterCondition(dm.filters.In, "name", dict(values=parameters[2]), is_edge_class=False),
            FilterCondition(dm.filters.Prefix, "name", dict(value=parameters[3]), is_edge_class=False),
            FilterCondition(dm.filters.Prefix, "externalId", dict(value=parameters[4]), is_edge_class=False),
            FilterCondition(dm.filters.Equals, "space", dict(value=parameters[5]), is_edge_class=False),
            FilterCondition(dm.filters.In, "space", dict(values=parameters[5]), is_edge_class=False),
        ],
    )

    # Act
    actual = FilterMethod.from_fields(data_class.fields, pygen_config.filtering)

    # Assert
    assert actual.parameters == expected.parameters
    for a, e in zip(actual.filters, expected.filters):
        assert a.filter == e.filter
        assert a.prop_name == e.prop_name
        assert a.keyword_arguments == e.keyword_arguments
        assert a == e
    assert actual == expected


def test_create_list_method_actors(actor_view: dm.View, pygen_config: PygenConfig) -> None:
    # Arrange
    data_class = NodeDataClass.from_view(actor_view, pygen_config.naming.data_class)

    person_data_class = MagicMock(spec=NodeDataClass)
    person_data_class.view_id = ViewSpaceExternalId(space="IntegrationTestsImmutable", external_id="Person")
    data_class.update_fields(
        actor_view.properties,
        {
            ViewSpaceExternalId(space="IntegrationTestsImmutable", external_id="Movie"): MagicMock(spec=NodeDataClass),
            ViewSpaceExternalId(space="IntegrationTestsImmutable", external_id="Nomination"): MagicMock(
                spec=NodeDataClass
            ),
            ViewSpaceExternalId(space="IntegrationTestsImmutable", external_id="Person"): person_data_class,
        },
        config=pygen_config,
    )
    parameters = [
        FilterParameter(
            "person",
            "str | tuple[str, str] | list[str] | list[tuple[str, str]]",
            space="IntegrationTestsImmutable",
            description="The person to filter on.",
        ),
        FilterParameter("won_oscar", "bool", description="The won oscar to filter on."),
        FilterParameter("external_id_prefix", "str", description="The prefix of the external ID to filter on."),
        FilterParameter("space", "str | list[str]", description="The space to filter on."),
    ]
    expected = FilterMethod(
        parameters=parameters,
        filters=[
            FilterConditionOnetoOneEdge(
                dm.filters.Equals, "person", dict(value=parameters[0]), instance_type=str, is_edge_class=False
            ),
            FilterConditionOnetoOneEdge(
                dm.filters.Equals, "person", dict(value=parameters[0]), instance_type=tuple, is_edge_class=False
            ),
            FilterConditionOnetoOneEdge(
                dm.filters.In, "person", dict(values=parameters[0]), instance_type=str, is_edge_class=False
            ),
            FilterConditionOnetoOneEdge(
                dm.filters.In, "person", dict(values=parameters[0]), instance_type=tuple, is_edge_class=False
            ),
            FilterCondition(dm.filters.Equals, "wonOscar", dict(value=parameters[1]), is_edge_class=False),
            FilterCondition(dm.filters.Prefix, "externalId", dict(value=parameters[2]), is_edge_class=False),
            FilterCondition(dm.filters.Equals, "space", dict(value=parameters[3]), is_edge_class=False),
            FilterCondition(dm.filters.In, "space", dict(values=parameters[3]), is_edge_class=False),
        ],
    )

    # Act
    actual = FilterMethod.from_fields(data_class.fields, pygen_config.filtering)

    # Assert
    assert actual.parameters == expected.parameters
    for a, e in zip(actual.filters, expected.filters):
        assert a.filter == e.filter
        assert a.prop_name == e.prop_name
        assert a.keyword_arguments == e.keyword_arguments
        assert a == e
    assert actual == expected
