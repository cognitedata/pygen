from typing import Literal

import pytest
from cognite.client import data_modeling as dm

from cognite import pygen
from cognite.pygen._core.create import properties_to_fields, properties_to_sources
from tests.constants import MovieSDKFiles


def test_create_view_data_classes(person_view: dm.View):
    # Arrange
    expected = MovieSDKFiles.persons_data.read_text()

    # Act
    actual = pygen.view_to_data_classes(person_view)

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
                "roles: list[str] = []",
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
