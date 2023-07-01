from __future__ import annotations

from pathlib import Path

import pytest

# from black import Mode, Report, WriteBack, reformat_one
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import ViewProperty

from cognite.pygen._core.sdk_generator import (
    APIGenerator,
    Field,
    SDKGenerator,
    client_subapi_import,
    subapi_instantiation,
)
from tests.constants import MovieSDKFiles, examples_dir


@pytest.fixture
def sdk_generator():
    return SDKGenerator("movie_domain.client", "MovieClient")


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
            write_type='Union[str, "RoleApply"]',
            is_edge=True,
            variable="role",
            dependency_class="Role",
            dependency_file="roles",
            edge_api_class_suffix="Roles",
        ),
        "list[str] = []",
        'list[Union[str, "RoleApply"]] = []',
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
            write_type='Union[str, "PersonApply"]',
            default="None",
            is_edge=True,
            dependency_class="Person",
            dependency_file="persons",
        ),
        "Optional[str] = None",
        'Optional[Union[str, "PersonApply"]] = None',
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


@pytest.mark.skip("Need to implement a AST comparison function as order of fields/methods/functions are not important")
def test_movie_model_to_sdk(sdk_generator: SDKGenerator, movie_model: dm.DataModel, tmp_path: Path):
    # Act
    files_by_path = sdk_generator.data_model_to_sdk(movie_model)

    # Assert
    for file_path, file_content in files_by_path.items():
        expected = (examples_dir / file_path).read_text()
        assert file_content == expected


def test_create_view_data_class_persons(person_view: dm.View):
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


def test_create_view_api_classes_actors(sdk_generator: SDKGenerator, actor_view: dm.View, tmp_path: Path):
    # Arrange
    expected = MovieSDKFiles.actors_api.read_text()

    # Act
    actual = sdk_generator.view_to_api(actor_view)

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


def test_create_api_classes(sdk_generator: SDKGenerator, monkeypatch):
    # Arrange
    expected = MovieSDKFiles.data_init.read_text()
    monkeypatch.setattr(
        sdk_generator,
        "_dependencies_by_view_name",
        {
            "Person": {"Role"},
            "Actor": {"Nomination", "Movie", "Person"},
            "Director": {"Nomination", "Movie", "Person"},
            "Movie": {"Actor", "Director", "Rating"},
            "Role": {"Nomination", "Movie", "Person"},
        },
    )
    monkeypatch.setattr(
        sdk_generator,
        "_view_names",
        {
            "Actor",
            "Person",
            "BestDirector",
            "BestLeadingActor",
            "BestLeadingActress",
            "Director",
            "Movie",
            "Nomination",
            "Rating",
            "Role",
        },
    )

    # Act
    actual = sdk_generator.create_data_classes_init()

    # Assert
    assert actual == expected


def test_create_api_client(sdk_generator: SDKGenerator, monkeypatch):
    # Arrange
    expected = MovieSDKFiles.client.read_text()
    monkeypatch.setattr(
        sdk_generator,
        "_view_names",
        {
            "Actor",
            "Person",
            "BestDirector",
            "BestLeadingActor",
            "BestLeadingActress",
            "Director",
            "Movie",
            "Nomination",
            "Rating",
            "Role",
        },
    )
    monkeypatch.setattr(sdk_generator, "_data_model_space", "IntegrationTestsImmutable")
    monkeypatch.setattr(sdk_generator, "_data_model_external_id", "Movie")
    monkeypatch.setattr(sdk_generator, "_data_model_version", "2")

    # Act
    actual = sdk_generator.create_api_client()

    # Assert
    assert actual == expected


@pytest.mark.parametrize(
    "view_name, expected",
    [
        ("Actor", "from ._api.actors import ActorsAPI"),
        ("BestDirector", "from ._api.best_directors import BestDirectorsAPI"),
    ],
)
def test_client_subapi_import(view_name: str, expected: str):
    # Act
    actual = client_subapi_import(view_name)

    # Assert
    assert actual == expected


@pytest.mark.parametrize(
    "view_name, expected",
    [
        ("Actor", "self.actors = ActorsAPI(client)"),
        ("BestDirector", "self.best_directors = BestDirectorsAPI(client)"),
    ],
)
def test_subapi_instantiation(view_name: str, expected: str):
    # Act
    actual = subapi_instantiation(view_name)

    # Assert
    assert actual == expected
