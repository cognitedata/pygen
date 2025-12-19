import pytest

from cognite.pygen.utils.text import to_camel, to_pascal, to_snake


@pytest.mark.parametrize(
    "word, singularize, pluralize, expected",
    [
        ("Actress", True, False, "Actress"),
        ("BestLeadingActress", True, False, "BestLeadingActress"),
        ("Actress", False, True, "Actresses"),
        ("Implementation1", False, False, "Implementation1"),
        ("Implementation1", False, True, "Implementations1"),
        ("Implementations1", True, False, "Implementation1"),
        ("pygen_model", True, False, "PygenModel"),
        ("pygen_models", False, False, "PygenModels"),
        ("pygen_model-power", False, False, "PygenModelPower"),
        ("pygen-model_power", False, False, "PygenModelPower"),
        ("1", False, False, "1"),
        ("CFIHOS_00000001", False, False, "CFIHOS00000001"),
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
        ("a_b", False, False, "aB"),
    ],
)
def test_to_camel(word: str, singularize: bool, pluralize: bool, expected: str):
    # Act
    actual = to_camel(word, singularize=singularize, pluralize=pluralize)

    # Assert
    assert actual == expected


@pytest.mark.parametrize(
    "word, singularize, pluralize, expected",
    [
        ("APM_Activity", False, True, "apm_activities"),
        ("APM_Material", False, True, "apm_materials"),
        ("Implementation1", False, False, "implementation_1"),
        ("power-models", False, False, "power_models"),
        ("object3D", False, False, "object_3d"),
    ],
)
def test_to_snake(word: str, singularize: bool, pluralize: bool, expected: str):
    # Act
    actual = to_snake(word, singularize=singularize, pluralize=pluralize)

    # Assert
    assert actual == expected
