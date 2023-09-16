from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import ViewProperty

from cognite.pygen._core.data_classes import (
    APIClass,
    Field,
    PrimitiveField,
    PrimitiveListField,
    OneEdgeField,
    ManyEdgeField,
    DataClass,
)
from cognite.pygen._core.generators import (
    APIGenerator,
    APIsGenerator,
    SDKGenerator,
    find_dependencies,
)
from cognite.pygen._generator import CodeFormatter
from cognite.pygen.config import PygenConfig
from tests.constants import IS_PYDANTIC_V1, MovieSDKFiles


@pytest.fixture
def top_level_package() -> str:
    if IS_PYDANTIC_V1:
        return "movie_domain_pydantic_v1.client"
    else:
        return "movie_domain.client"


@pytest.fixture
def sdk_generator(movie_model, top_level_package) -> SDKGenerator:
    return SDKGenerator(top_level_package, "MovieClient", movie_model)


@pytest.fixture
def apis_generator(movie_model, top_level_package) -> APIsGenerator:
    return APIsGenerator(top_level_package, "MovieClient", movie_model.views)


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
        PrimitiveField(
            name="name",
            prop_name="name",
            type_="str",
            prop=cast(dm.MappedProperty, prop),
            is_nullable=False,
            default=None,
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
    data_class = DataClass(
        read_class_name="Role",
        write_class_name="RoleApply",
        read_list_class_name="RoleList",
        write_list_class_name="RoleListApply",
        variable_name="role",
        file_name="_roles",
        view_id=dm.ViewId("IntegrationTestsImmutable", "Role", "2"),
    )

    data_class_by_view_id = {dm.ViewId("IntegrationTestsImmutable", "Role", "2"): data_class}
    yield pytest.param(
        "roles",
        prop,
        data_class_by_view_id,
        ManyEdgeField(
            name="roles",
            prop_name="roles",
            prop=cast(dm.SingleHopConnectionDefinition, prop),
            data_class=data_class,
        ),
        "list[str] = []",
        "Union[list[RoleApply], list[str]] = Field(default_factory=list, repr=False)",
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
        PrimitiveListField(
            name="configs",
            prop_name="configs",
            prop=cast(dm.MappedProperty, prop),
            type_="str",
            is_nullable=False,
        ),
        "list[str] = Field(default_factory=list)",
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
    data_class = DataClass(
        read_class_name="Person",
        write_class_name="PersonApply",
        read_list_class_name="PersonList",
        write_list_class_name="PersonListApply",
        variable_name="person",
        file_name="_persons",
        view_id=dm.ViewId("IntegrationTestsImmutable", "Person", "2"),
    )
    data_class_by_view_id = {dm.ViewId("IntegrationTestsImmutable", "Person", "2"): data_class}

    prop = ViewProperty.load(prop)
    yield pytest.param(
        "person",
        prop,
        data_class_by_view_id,
        OneEdgeField(
            name="person",
            prop_name="person",
            prop=cast(dm.MappedProperty, prop),
            data_class=data_class,
        ),
        "Optional[str] = None",
        'Optional[Union["PersonApply", str]] = Field(None, repr=False)',
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
        PrimitiveField(
            name="won_oscar",
            prop_name="wonOscar",
            prop=cast(dm.MappedProperty, prop),
            is_nullable=True,
            default="None",
            type_="bool",
        ),
        'Optional[bool] = Field(None, alias="wonOscar")',
        "Optional[bool] = None",
        id="Boolean property with pascal name",
    )


@pytest.mark.parametrize(
    "prop_name, property_, data_class_by_view_id, expected, expected_read_type_hint, expected_write_type_hint",
    list(create_fields_test_cases()),
)
def test_fields_from_property(
    prop_name: str,
    property_: dm.MappedProperty | dm.ConnectionDefinition,
    data_class_by_view_id: dict[dm.ViewId, DataClass],
    expected: Field,
    expected_read_type_hint: str,
    expected_write_type_hint: str,
    pygen_config: PygenConfig,
):
    # Act
    actual = Field.from_property(prop_name, property_, data_class_by_view_id, pygen_config)

    # Assert
    assert actual == expected
    assert actual.as_read_type_hint() == expected_read_type_hint
    assert actual.as_write_type_hint() == expected_write_type_hint


def test_generate_data_class_file_persons(person_view: dm.View, pygen_config: PygenConfig):
    # Arrange
    expected = MovieSDKFiles.persons_data.read_text()

    # Act
    actual = APIGenerator(person_view, pygen_config).generate_data_class_file()

    # Assert
    assert actual == expected


def test_create_view_data_class_actors(actor_view: dm.View, top_level_package: str):
    # Arrange
    expected = MovieSDKFiles.actors_data.read_text()

    # Act
    actual = APIGenerator(actor_view, top_level_package).generate_data_class_file()

    # Assert
    assert actual == expected


def test_create_view_api_classes_actors(actor_view: dm.View, top_level_package: str, tmp_path: Path):
    # Arrange
    expected = MovieSDKFiles.actors_api.read_text()

    # Act
    actual = APIGenerator(actor_view, top_level_package).generate_api_file(top_level_package)

    # Assert
    # Reformat with black to make sure the formatting is correct
    # reformat_one(
    #     tmp_actors,
    #
    #     ),

    assert actual == expected


def test_create_view_api_classes_persons(person_view: dm.View, top_level_package: str):
    # Arrange
    expected = MovieSDKFiles.persons_api.read_text()

    # Act
    actual = APIGenerator(person_view, top_level_package).generate_api_file(top_level_package)

    # Assert
    assert actual == expected


def test_create_api_classes(apis_generator: APIsGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = MovieSDKFiles.data_init.read_text()

    # Act
    actual = apis_generator.generate_data_classes_init_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_api_client(sdk_generator: SDKGenerator):
    # Arrange
    expected = MovieSDKFiles.client.read_text()

    # Act
    actual = sdk_generator._generate_api_client_file()

    # Assert
    assert actual == expected


def test_find_dependencies(movie_model: dm.DataModel, top_level_package: str):
    # Arrange
    views = dm.ViewList(movie_model.views)
    apis = [APIGenerator(view, top_level_package) for view in views]
    expected = {
        APIClass.from_view(k, top_level_package): {APIClass.from_view(v, top_level_package) for v in values}
        for k, values in {
            "Person": {"Role"},
            "Actor": {"Nomination", "Movie", "Person"},
            "Director": {"Nomination", "Movie", "Person"},
            "Movie": {"Actor", "Director", "Rating"},
            "Role": {"Nomination", "Movie", "Person"},
        }.items()
    }

    # Act
    actual = find_dependencies(apis)

    # Assert
    assert actual == expected
