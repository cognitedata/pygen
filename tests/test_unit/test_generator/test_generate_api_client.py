import platform

import pytest

from cognite.pygen._core.generators import SDKGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import MULTI_MODEL_SDK, MultiModelFiles


@pytest.mark.skipif(
    not platform.platform().startswith("Windows"),
    reason="There is currently some strange problem with the diff on non-windows",
)
def test_generate_api_client(code_formatter: CodeFormatter):
    # Arrange
    sdk_generator = SDKGenerator(
        MULTI_MODEL_SDK.top_level_package,
        MULTI_MODEL_SDK.client_name,
        MULTI_MODEL_SDK.load_data_models(),
    )
    expected = MultiModelFiles.api_client.read_text()

    # Act
    actual = sdk_generator._generate_api_client_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected
