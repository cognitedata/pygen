import pytest

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from equipment_unit.client import EquipmentUnitClient
else:
    from equipment_unit_pydantic_v1.client import EquipmentUnitClient


@pytest.fixture()
def workorder(client_config) -> EquipmentUnitClient:
    return EquipmentUnitClient.azure_project(**client_config)
