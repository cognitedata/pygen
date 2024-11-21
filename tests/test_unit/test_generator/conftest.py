import pytest
from cognite.client import data_modeling as dm

from tests.constants import PUMP_SDK


@pytest.fixture(scope="session")
def pump_model() -> dm.DataModel[dm.View]:
    return PUMP_SDK.load_data_model()
