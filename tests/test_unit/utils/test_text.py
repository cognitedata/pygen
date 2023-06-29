import pytest

from cognite.pygen.utils.text import to_pascal


@pytest.mark.parametrize(
    "word, singularize, pluralize, expected",
    [
        (
            "Actress",
            True,
            False,
            "Actress",
        ),
        ("BestLeadingActress", True, False, "BestLeadingActress"),
        (
            "Actress",
            False,
            True,
            "Actresses",
        ),
    ],
)
def test_to_pascal(word: str, singularize: bool, pluralize: bool, expected: str):
    # Act
    actual = to_pascal(word, singularize=singularize, pluralize=pluralize)

    # Assert
    assert actual == expected
