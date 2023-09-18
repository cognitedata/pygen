import pytest

from cognite.pygen.utils.text import to_pascal, to_snake


@pytest.mark.parametrize(
    "word, singularize, pluralize, expected",
    [
        ("Actress", True, False, "Actress"),
        ("BestLeadingActress", True, False, "BestLeadingActress"),
        ("Actress", False, True, "Actresses"),
    ],
)
def test_to_pascal(word: str, singularize: bool, pluralize: bool, expected: str):
    # Act
    actual = to_pascal(word, singularize=singularize, pluralize=pluralize)

    # Assert
    assert actual == expected


@pytest.mark.parametrize(
    "word, singularize, pluralize, expected",
    [
        ("APM_Activity", False, True, "apm_activities"),
        ("APM_Material", False, True, "apm_materials"),
    ],
)
def test_to_snake(word: str, singularize: bool, pluralize: bool, expected: str):
    # Act
    actual = to_snake(word, singularize=singularize, pluralize=pluralize)

    # Assert
    assert actual == expected
