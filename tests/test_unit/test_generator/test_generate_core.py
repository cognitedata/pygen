import platform

import pytest

from cognite.pygen._core.generators import MultiAPIGenerator, SDKGenerator
from cognite.pygen._generator import CodeFormatter
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


def test_generate_data_class_init_file(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = OmniFiles.data_init.read_text()

    # Act
    actual = omni_multi_api_generator.generate_data_classes_init_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


@pytest.mark.skipif(
    not platform.platform().startswith("Windows"),
    reason="There is currently some strange problem with the diff on non-windows",
)
def test_create_api_client(omni_sdk_generator: SDKGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = OmniFiles.client.read_text()

    # Act
    actual = omni_sdk_generator._generate_api_client_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected
