from cognite.client import data_modeling as dm

from cognite import pygen
from tests.constants import MovieSDKFiles


def test_create_view_data_classes(person_view: dm.View):
    # Arrange
    expected = MovieSDKFiles.persons_data.read_text()

    # Act
    actual = pygen.view_to_data_classes(person_view)

    # Assert
    assert actual == expected
