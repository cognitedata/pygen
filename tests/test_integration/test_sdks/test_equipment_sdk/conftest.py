import pytest

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from equipment_unit import EquipmentUnitClient
else:
    from equipment_unit_pydantic_v1 import EquipmentUnitClient


@pytest.fixture()
def workorder(client_config) -> EquipmentUnitClient:
    return EquipmentUnitClient.azure_project(**client_config)
