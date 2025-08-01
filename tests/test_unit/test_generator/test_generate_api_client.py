import difflib

from black import InvalidInput

from cognite.pygen._core.generators import SDKGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import OMNI_MULTI_SDK, OMNI_SUB_SDK, OmniMultiFiles, OmniSubFiles


def test_generate_multi_api_client(code_formatter: CodeFormatter):
    # Arrange
    sdk_generator = SDKGenerator(
        OMNI_MULTI_SDK.top_level_package,
        OMNI_MULTI_SDK.client_name,
        OMNI_MULTI_SDK.load_data_models(),
    )
    expected = OmniMultiFiles.api_client.read_text()

    # Act
    actual = sdk_generator._generate_api_client_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected, "\n".join(difflib.unified_diff(expected.splitlines(), actual.splitlines()))


def test_generate_multi_model_api_client_default_instance_space_valid_python_syntax(code_formatter: CodeFormatter):
    # Arrange
    sdk_generator = SDKGenerator(
        top_level_package=OMNI_MULTI_SDK.top_level_package,
        client_name=OMNI_MULTI_SDK.client_name,
        data_model=OMNI_MULTI_SDK.load_data_models(),
        default_instance_space="default_instance_space",
    )

    # Act
    actual = sdk_generator._generate_api_client_file()

    try:
        _ = code_formatter.format_code(actual)
    except InvalidInput as e:
        raise AssertionError(f"Code formatting failed: {e!s}") from None
    assert True


def test_generate_single_api_client_no_default_instance_space(code_formatter: CodeFormatter):
    # Arrange
    sdk_generator = SDKGenerator(
        OMNI_SUB_SDK.top_level_package,
        OMNI_SUB_SDK.client_name,
        OMNI_SUB_SDK.load_data_models(),
    )
    expected = OmniSubFiles.api_client.read_text()

    # Act
    actual = sdk_generator._generate_api_client_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected, "\n".join(difflib.unified_diff(expected.splitlines(), actual.splitlines()))
