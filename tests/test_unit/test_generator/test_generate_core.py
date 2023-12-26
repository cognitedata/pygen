from cognite.pygen._core.generators import MultiAPIGenerator
from tests.constants import OmniFiles


def test_generate_data_class_core(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.core_data.read_text()

    # Act
    actual = omni_multi_api_generator.generate_data_class_core_file()

    # Assert
    assert actual == expected


def test_generate_api_core(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.core_api.read_text()

    # Act
    actual = omni_multi_api_generator.generate_api_core_file()

    # Assert
    assert actual == expected
