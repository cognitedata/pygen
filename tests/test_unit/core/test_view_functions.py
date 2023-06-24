from cognite.client import data_modeling as dm

from cognite.pygen._core import view_functions


def test_one_to_many_properties_person(person_view: dm.View):
    # Arrange
    expected = [person_view.properties["roles"]]
    # Act
    actual = view_functions.one_to_many_properties(person_view.properties.values())

    # Assert
    assert list(actual) == expected


def test_one_to_many_properties_actor(actor_view: dm.View):
    # Arrange
    expected = [actor_view.properties["movies"], actor_view.properties["nomination"]]

    # Act
    actual = view_functions.one_to_many_properties(actor_view.properties.values())

    # Assert
    assert list(actual) == expected
