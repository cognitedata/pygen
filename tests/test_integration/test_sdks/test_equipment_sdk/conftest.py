import pytest

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from examples.equipment_unit.client import EquipmentUnitClient
else:
    raise NotImplementedError()


@pytest.fixture()
def equipment_client(client_config) -> EquipmentUnitClient:
    return EquipmentUnitClient.azure_project(**client_config)
