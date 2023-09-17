import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import SDKGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import APM_SDK, EXAMPLES_DIR


@pytest.fixture
def sdk_generator(apm_data_model: dm.DataModel[dm.View]) -> SDKGenerator:
    return SDKGenerator(
        APM_SDK.top_level_package,
        APM_SDK.client_name,
        apm_data_model,
    )


def test_generate_sdk(sdk_generator: SDKGenerator, code_formatter: CodeFormatter) -> None:
    # Act
    sdk = sdk_generator.generate_sdk()

    # Assert
    for file_path, actual in sdk.items():
        expected_location = EXAMPLES_DIR / file_path
        assert expected_location.exists()
        expected = expected_location.read_text()

        actual = code_formatter.format_code(actual)
        assert actual == expected
