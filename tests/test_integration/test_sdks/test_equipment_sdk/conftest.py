import pytest
from equipment_unit import EquipmentUnitClient


@pytest.fixture()
def workorder(client_config) -> EquipmentUnitClient:
    return EquipmentUnitClient.azure_project(**client_config)
