from typing import Literal

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.sdk_generator import (
    EdgeSnippets,
    SDKGenerator,
    dependencies_to_imports,
    properties_to_fields,
    properties_to_sources,
    property_to_edge_snippets,
)
from tests.constants import MovieSDKFiles


@pytest.fixture
def sdk_generator():
    return SDKGenerator("movie_domain")


def test_create_view_api_classes(sdk_generator: SDKGenerator, person_view: dm.View):
    # Arrange
    expected = MovieSDKFiles.persons_api.read_text()

    # Act
    actual = sdk_generator.view_to_api(person_view)

    # Assert
    assert actual == expected


def test_create_view_data_classes(sdk_generator: SDKGenerator, person_view: dm.View):
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


def test_property_to_edge_api(sdk_generator: SDKGenerator, person_view: dm.View):
    # Arrange
    expected = "\n".join(MovieSDKFiles.persons_api.read_text().split("\n")[14:38])

    # Act
    actual = sdk_generator.property_to_edge_api(
        person_view.properties["roles"], view_name="Person", view_space="IntegrationTestsImmutable"
    )

    # Assert
    assert actual == expected


def test_property_to_edge_helper(sdk_generator: SDKGenerator, person_view: dm.View):
    # Arrange
    expected = "\n".join(MovieSDKFiles.persons_api.read_text().split("\n")[88:98])

    # Act
    actual = sdk_generator.property_to_edge_helper(person_view.properties["roles"], view_name="Person")

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


@pytest.mark.parametrize(
    "field_type, expected",
    [
        (
            "read",
            [
                "name: Optional[str] = None",
                'birth_year: Optional[int] = Field(None, alias="birthYear")',
                "roles: list[str] = []",
            ],
        ),
        (
            "write",
            [
                "name: str",
                "birth_year: Optional[int] = None",
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
                        "name": self.name,
                        "birthYear": self.birth_year,
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
