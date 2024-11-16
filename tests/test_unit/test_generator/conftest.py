import pytest
from cognite.client import data_modeling as dm

from tests.constants import (
    EQUIPMENT_UNIT_SDK,
    PUMP_SDK,
)


@pytest.fixture(scope="session")
def pump_model() -> dm.DataModel[dm.View]:
    return PUMP_SDK.load_data_model()


@pytest.fixture(scope="session")
def equipment_unit_model() -> dm.DataModel[dm.View]:
    return EQUIPMENT_UNIT_SDK.load_data_model()


@pytest.fixture(scope="session")
def unit_procedure_view(equipment_unit_model: dm.DataModel) -> dm.View:
    return next(v for v in equipment_unit_model.views if v.name == "UnitProcedure")


@pytest.fixture(scope="session")
def equipment_module_view(equipment_unit_model: dm.DataModel) -> dm.View:
    return next(v for v in equipment_unit_model.views if v.name == "EquipmentModule")


@pytest.fixture(scope="session")
def start_end_time_view(equipment_unit_model: dm.DataModel) -> dm.View:
    return next(v for v in equipment_unit_model.views if v.name == "StartEndTime")
