import difflib

from cognite.pygen._core.generators import MultiAPIGenerator, SDKGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import OmniFiles, OmniSubFiles


def test_generate_data_class_core_base(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.data_core_base.read_text()

    # Act
    actual = omni_multi_api_generator.generate_data_class_core_base_file()

    # Assert
    assert actual == expected


def test_generate_data_class_core_constants(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.data_core_constants.read_text()

    # Act
    actual = omni_multi_api_generator.generate_data_class_core_constants_file()

    # Assert
    assert actual == expected


def test_generate_data_class_core_init(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.data_core_init.read_text()

    # Act
    actual = omni_multi_api_generator.generate_data_class_core_init_file()

    # Assert
    assert actual == expected


def test_generate_data_class_core_helpers(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.data_core_helpers.read_text()

    # Act
    actual = omni_multi_api_generator.generate_data_class_core_helpers_file()

    # Assert
    assert actual == expected


def test_generate_data_class_core_query_init(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.data_core_query_init.read_text()

    # Act
    actual = omni_multi_api_generator.generate_data_class_core_query_init()

    # Assert
    assert actual == expected


def test_generate_data_class_core_query_filter_classes(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.data_core_query_filter_classes.read_text()

    # Act
    actual = omni_multi_api_generator.generate_data_class_core_query_filter_classes()

    # Assert
    assert actual == expected


def test_generate_data_class_core_query_select(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.data_core_query_select.read_text()

    # Act
    actual = omni_multi_api_generator.generate_data_class_core_query_select()

    # Assert
    assert actual == expected


def test_generate_data_class_query_files(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    content_by_file = omni_multi_api_generator.generate_data_class_core_query_files()
    for filename, actual in content_by_file.items():
        expected = (OmniFiles.core_query_data / filename).read_text()

        # Assert
        assert actual == expected, f"File: {filename}"


def test_generate_data_class_core_cdf_external(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.data_core_cdf_external.read_text()

    # Act
    actual = omni_multi_api_generator.generate_data_class_core_cdf_external_file()

    # Assert
    assert actual == expected


def test_generate_data_class_core_base_no_default_space(omnisub_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniSubFiles.data_core_base.read_text()

    # Act
    actual = omnisub_multi_api_generator.generate_data_class_core_base_file()

    # Assert
    assert actual == expected


def test_generate_data_class_core_constant_no_default_space(omnisub_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniSubFiles.data_core_constants.read_text()

    # Act
    actual = omnisub_multi_api_generator.generate_data_class_core_constants_file()

    # Assert
    assert actual == expected


def test_generate_data_class_core_helpers_no_default_space(omnisub_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniSubFiles.data_core_helpers.read_text()

    # Act
    actual = omnisub_multi_api_generator.generate_data_class_core_helpers_file()

    # Assert
    assert actual == expected


def test_generate_api_core(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.core_api.read_text()

    # Act
    actual = omni_multi_api_generator.generate_api_core_file()

    # Assert
    assert actual == expected


def test_generate_api_core_no_default_space(omnisub_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniSubFiles.core_api.read_text()

    # Act
    actual = omnisub_multi_api_generator.generate_api_core_file()

    # Assert
    assert actual == expected


def test_generate_api_init(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected = OmniFiles.core_init.read_text()

    # Act
    actual = omni_multi_api_generator.generate_api_init_file()

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


def test_create_api_client(omni_sdk_generator: SDKGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = OmniFiles.client.read_text()

    # Act
    actual = omni_sdk_generator._generate_api_client_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected, "\n".join(difflib.unified_diff(expected.splitlines(), actual.splitlines()))


def test_generate_config_py(omni_sdk_generator: SDKGenerator) -> None:
    # Arrange
    expected = OmniFiles.config.read_text()

    # Act
    actual = omni_sdk_generator._generate_config_file()

    # Assert
    assert actual == expected
