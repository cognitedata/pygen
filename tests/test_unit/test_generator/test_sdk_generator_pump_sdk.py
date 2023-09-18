import pytest
from cognite.client import data_modeling as dm

from cognite.pygen import SDKGenerator
from cognite.pygen.exceptions import NameConflict


@pytest.fixture(scope="session")
def sdk_generator(pump_model: dm.DataModel[dm.View]) -> SDKGenerator:
    return SDKGenerator("cognite", "pump", pump_model)


def test_generate_sdk_raises_name_conflict(sdk_generator: SDKGenerator) -> None:
    # Act
    with pytest.raises(NameConflict) as e:
        sdk_generator.generate_sdk()

    # Assert
    assert "Name conflict detected. The following names are used multiple times: ['PumpClient']" in str(e.value)
