from __future__ import annotations

from pathlib import Path

import pytest

# from black import Mode, Report, WriteBack, reformat_one
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import ViewProperty

from cognite.pygen._core.dms_to_python import APIClass, APIGenerator, Field, SDKGenerator, find_dependencies
from tests.constants import MovieSDKFiles


@pytest.fixture
def sdk_generator(movie_model):
    return SDKGenerator("movie_domain.client", "MovieClient", movie_model)


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
    yield pytest.param(
        prop,
        Field(
            name="name",
            prop=prop,
            read_type="str",
            is_nullable=False,
            is_list=False,
            default=None,
            write_type="str",
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
    yield pytest.param(
        prop,
        Field(
            name="roles",
            prop=prop,
            read_type="str",
            is_list=True,
            is_nullable=True,
            default="[]",
            write_type='Union["RoleApply", str]',
            is_edge=True,
            variable="role",
            dependency_class="Role",
            dependency_file="roles",
            edge_api_class_suffix="Roles",
            edge_api_attribute="roles",
        ),
        "list[str] = []",
        'list[Union["RoleApply", str]] = Field(default_factory=list, repr=False)',
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
        prop,
        Field(
            name="configs",
            prop=prop,
            read_type="str",
            is_list=True,
            is_nullable=False,
            default="[]",
            write_type="str",
            variable="config",
        ),
        "list[str] = []",
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
    prop = ViewProperty.load(prop)
    yield pytest.param(
        prop,
        Field(
            name="person",
            prop=prop,
            read_type="str",
            is_list=False,
            is_nullable=True,
            write_type='Union["PersonApply", str]',
            default="None",
            is_edge=True,
            dependency_class="Person",
            dependency_file="persons",
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
        prop,
        Field(
            name="won_oscar",
            prop=prop,
            read_type="bool",
            is_list=False,
            is_nullable=True,
            write_type="bool",
            default="None",
        ),
        'Optional[bool] = Field(None, alias="wonOscar")',
        "Optional[bool] = None",
        id="Boolean property with pascal name",
    )


@pytest.mark.parametrize(
    "property_, expected, expected_read_type_hint,expected_write_type_hint", list(create_fields_test_cases())
)
def test_fields_from_property(
    property_: dm.MappedProperty | dm.ConnectionDefinition,
    expected: Field,
    expected_read_type_hint: str,
    expected_write_type_hint: str,
):
    # Act
    actual = Field.from_property(property_)

    # Assert
    assert actual == expected
    assert actual.as_type_hint("read") == expected_read_type_hint
    assert actual.as_type_hint("write") == expected_write_type_hint


def test_generate_data_class_file_persons(person_view: dm.View):
    # Arrange
    expected = MovieSDKFiles.persons_data.read_text()

    # Act
    actual = APIGenerator(person_view).generate_data_class_file()

    # Assert
    assert actual == expected


def test_create_view_data_class_actors(actor_view: dm.View):
    # Arrange
    expected = MovieSDKFiles.actors_data.read_text()

    # Act
    actual = APIGenerator(actor_view).generate_data_class_file()

    # Assert
    assert actual == expected


def test_create_view_api_classes_actors(actor_view: dm.View, tmp_path: Path):
    # Arrange
    expected = MovieSDKFiles.actors_api.read_text()

    # Act
    actual = APIGenerator(actor_view).generate_api_file("movie_domain.client")

    # Assert
    # Reformat with black to make sure the formatting is correct
    # tmp_actors = tmp_path / "actors.py"
    # tmp_actors.write_text(actual)
    # reformat_one(
    #     tmp_actors,
    #     fast=True,
    #     write_back=WriteBack.YES,
    #     mode=Mode(
    #         target_versions={"py39"},
    #         line_length=120
    #
    #     ),
    #     report=Report(quiet=True),
    # )
    # actual = tmp_actors.read_text()

    assert actual == expected


def test_create_view_api_classes_persons(sdk_generator: SDKGenerator, person_view: dm.View):
    # Arrange
    expected = MovieSDKFiles.persons_api.read_text()

    # Act
    actual = APIGenerator(person_view).generate_api_file("movie_domain.client")

    # Assert
    assert actual == expected


def test_create_api_classes(sdk_generator: SDKGenerator):
    # Arrange
    expected = MovieSDKFiles.data_init.read_text()

    # Act
    actual = sdk_generator.generate_data_classes_init_file()

    # Assert
    assert actual == expected


def test_create_api_client(sdk_generator: SDKGenerator):
    # Arrange
    expected = MovieSDKFiles.client.read_text()

    # Act
    actual = sdk_generator.generate_api_client_file()

    # Assert
    assert actual == expected


def test_find_dependencies(movie_model: dm.DataModel):
    # Arrange
    views = dm.ViewList(movie_model.views)
    apis = [APIGenerator(view) for view in views]
    expected = {
        APIClass.from_view(k): {APIClass.from_view(v) for v in values}
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
