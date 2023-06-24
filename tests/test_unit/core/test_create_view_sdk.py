from cognite.client import data_modeling as dm

from cognite import pygen
from cognite.pygen._core.create import properties_to_read_fields
from tests.constants import MovieSDKFiles


def test_create_view_data_classes(person_view: dm.View):
    # Arrange
    expected = MovieSDKFiles.persons_data.read_text()

    # Act
    actual = pygen.view_to_data_classes(person_view)

    # Assert
    assert actual == expected


def test_create_read_properties(person_view):
    # Arrange
    expected = [
        "name: Optional[str] = None",
        'birth_year: Optional[int] = Field(None, alias="birthYear")',
        "roles: list[str] = []",
    ]

    # Act
    actual = properties_to_read_fields(person_view.properties.values())

    # Assert
    assert actual == expected
