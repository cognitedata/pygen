import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.dms_to_python import SDKGenerator
from tests.constants import APM_SDK


@pytest.fixture
def sdk_generator(apm_data_model: dm.DataModel[dm.View]) -> SDKGenerator:
    return SDKGenerator(
        APM_SDK.top_level_package,
        APM_SDK.client_name,
        [apm_data_model],
    )


def test_generate_sdk(sdk_generator: SDKGenerator):
    # Act
    sdk = sdk_generator.generate_sdk()

    # Assert
    for file_path, actual in sdk.values():
        expected_location = APM_SDK.client_dir / file_path
        assert expected_location.exists()
        expected = expected_location.read_text()

        assert actual == expected
