import platform

import pytest

from cognite.pygen._core.generators import SDKGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import OMNI_MULTI_SDK, OmniMultiFiles


@pytest.mark.skipif(
    not platform.platform().startswith("Windows"),
    reason="There is currently some strange problem with the diff on non-windows",
)
def test_generate_api_client(code_formatter: CodeFormatter):
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
    assert actual == expected
