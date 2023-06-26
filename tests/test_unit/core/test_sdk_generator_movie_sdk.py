from pathlib import Path
from typing import Literal

import pytest

# from black import Mode, Report, WriteBack, reformat_one
from cognite.client import data_modeling as dm

from cognite.pygen._core.sdk_generator import (
    EdgeSnippets,
    SDKGenerator,
    client_subapi_import,
    dependencies_to_imports,
    properties_to_fields,
    properties_to_sources,
    property_to_edge_snippets,
    subapi_instantiation,
)
from tests.constants import MovieSDKFiles, examples_dir


@pytest.fixture
def sdk_generator():
    return SDKGenerator("movie_domain", "Movie")


@pytest.mark.skip("Need to implement a AST comparison function as order of fields/methods/functions are not important")
def test_movie_model_to_sdk(sdk_generator: SDKGenerator, movie_model: dm.DataModel, tmp_path: Path):
    # Act
    files_by_path = sdk_generator.data_model_to_sdk(movie_model)

    # Assert
    for file_path, file_content in files_by_path.items():
        expected = (examples_dir / file_path).read_text()
        assert file_content == expected


def test_create_view_api_classes_persons(sdk_generator: SDKGenerator, person_view: dm.View):
    # Arrange
    expected = MovieSDKFiles.persons_api.read_text()

    # Act
    actual = sdk_generator.view_to_api(person_view)

    # Assert
    assert actual == expected


def test_create_view_data_class_persons(sdk_generator: SDKGenerator, person_view: dm.View):
    # Arrange
    expected = MovieSDKFiles.persons_data.read_text()

    # Act
    actual = sdk_generator.view_to_data_classes(person_view)

    # Assert
    assert actual == expected


def test_create_view_data_class_actors(sdk_generator: SDKGenerator, actor_view: dm.View):
    # Arrange
    expected = MovieSDKFiles.actors_data.read_text()

    # Act
    actual = sdk_generator.view_to_data_classes(actor_view)

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


def test_property_to_edge_api_person_roles(sdk_generator: SDKGenerator, person_view: dm.View):
    # Arrange
    expected = "\n".join(MovieSDKFiles.persons_api.read_text().split("\n")[14:45])

    # Act
    actual = sdk_generator.property_to_edge_api(
        person_view.properties["roles"], view_name="Person", view_space="IntegrationTestsImmutable"
    )

    # Assert
    assert actual == expected


def test_property_to_edge_api_actor_person(sdk_generator: SDKGenerator, actor_view: dm.View):
    # Arrange
    expected = "\n".join(MovieSDKFiles.actors_api.read_text().split("\n")[14:45])

    # Act
    actual = sdk_generator.property_to_edge_api(
        actor_view.properties["person"], view_name="Actor", view_space="IntegrationTestsImmutable"
    )

    # Assert
    assert actual == expected


def test_property_to_edges_helper(sdk_generator: SDKGenerator, person_view: dm.View):
    # Arrange
    expected = "\n".join(MovieSDKFiles.persons_api.read_text().split("\n")[98:108])

    # Act
    actual = sdk_generator.property_to_edge_helper(person_view.properties["roles"], view_name="Person")

    # Assert
    assert actual == expected


def test_property_to_edge_helper(sdk_generator: SDKGenerator, actor_view: dm.View):
    # Arrange
    expected = "\n".join(MovieSDKFiles.actors_api.read_text().split("\n")[178:186])

    # Act
    actual = sdk_generator.property_to_edge_helper(actor_view.properties["person"], view_name="Actor")

    # Assert
    assert actual == expected


def test_property_to_edge_snippets(person_view: dm.View):
    # Arrange
    expected = EdgeSnippets(
        "self.roles = PersonRolesAPI(client)",
        "person.roles = [edge.end_node.external_id for edge in role_edges]",
        "self._set_roles(persons, role_edges)",
        "role_edges = self.roles.retrieve(external_id)",
        "role_edges = self.roles.list(limit=-1)",
    )

    # Act
    actual = property_to_edge_snippets(person_view.properties["roles"], view_name="Person")

    # Assert
    assert actual == expected


def test_property_to_single_edge_snippets(actor_view: dm.View):
    # Arrange
    expected = EdgeSnippets(
        "self.person = ActorPersonAPI(client)",
        "actor.person = person_edges[0].end_node.external_id if person_edges else None",
        "self._set_person(actors, person_edges)",
        "person_edges = self.person.retrieve(external_id)",
        "person_edges = self.person.list(limit=-1)",
    )

    # Act
    actual = property_to_edge_snippets(actor_view.properties["person"], view_name="Actor")

    # Assert
    assert actual == expected


@pytest.mark.parametrize(
    "field_type, expected",
    [
        (
            "read",
            [
                'birth_year: Optional[int] = Field(None, alias="birthYear")',
                "name: Optional[str] = None",
                "roles: list[str] = []",
            ],
        ),
        (
            "write",
            [
                "birth_year: Optional[int] = None",
                "name: str",
                'roles: list[Union[str, "RoleApply"]] = []',
            ],
        ),
    ],
)
def test_create_fields(field_type: Literal["read", "write"], expected: list[str], person_view):
    # Act
    actual = properties_to_fields(person_view.properties.values())

    # Assert
    assert [f.as_type_hint(field_type) for f in actual] == expected


def test_create_sources(person_view: dm.View):
    # Arrange
    expected = [
        """dm.NodeOrEdgeData(
                    source=dm.ContainerId("IntegrationTestsImmutable", "Person"),
                    properties={
                        "birthYear": self.birth_year,
                        "name": self.name,
                    },
                ),"""
    ]

    # Act
    actual = properties_to_sources(person_view.properties.values())

    # Assert
    assert actual == expected


def dependencies_to_imports_test_cases():
    expected = """if TYPE_CHECKING:
    from ._roles import RoleApply
"""
    yield pytest.param({"Role"}, expected, id="single dependency")
    expected = """if TYPE_CHECKING:
    from ._movies import MovieApply
    from ._nominations import NominationApply
"""
    yield pytest.param({"Movie", "Nomination"}, expected, id="multiple dependencies")


@pytest.mark.parametrize("dependencies, expected", list(dependencies_to_imports_test_cases()))
def test_dependencies_to_imports(dependencies: set[str], expected: str):
    # Act
    actual = dependencies_to_imports(dependencies)

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


def test_properties_to_create_edge_methods_persons(sdk_generator: SDKGenerator, person_view: dm.View):
    # Arrange
    expected = ["\n".join(MovieSDKFiles.persons_data.read_text().split("\n")[62:77])]

    # Act
    actual = sdk_generator.properties_to_create_edge_methods(person_view.properties.values())

    # Assert
    assert actual == expected


def test_properties_to_add_edges_persons(sdk_generator: SDKGenerator, person_view: dm.View):
    # Arrange
    expected = ["\n".join(MovieSDKFiles.persons_data.read_text().split("\n")[49:59])]

    # Act
    actual = sdk_generator.properties_to_add_edges(person_view.properties.values())

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
