import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.dms_to_python import SDKGenerator
from tests.constants import APM_SDK


@pytest.fixture
def sdk_generator(shop_model: dm.DataModel, top_level_package: str):
    return SDKGenerator(
        APM_SDK.top_level_package,
        APM_SDK.client_name,
    )
