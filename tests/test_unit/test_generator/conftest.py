import pytest
from cognite.client import data_modeling as dm

from tests.constants import (
    APM_SDK,
    EQUIPMENT_UNIT_SDK,
    MARKET_SDK,
    MOVIE_SDK,
    PUMP_SDK,
    SCENARIO_INSTANCE_SDK,
    SHOP_SDK,
)


@pytest.fixture(scope="session")
def movie_model() -> dm.DataModel[dm.View]:
    return MOVIE_SDK.load_data_model()


@pytest.fixture(scope="session")
def person_view(movie_model: dm.DataModel) -> dm.View:
    return next(v for v in movie_model.views if v.name == "Person")


@pytest.fixture(scope="session")
def actor_view(movie_model: dm.DataModel) -> dm.View:
    return next(v for v in movie_model.views if v.name == "Actor")


@pytest.fixture(scope="session")
def movie_views(movie_model: dm.DataModel) -> dm.ViewList:
    return dm.ViewList(movie_model.views)


@pytest.fixture
def shop_model() -> dm.DataModel[dm.DataModel]:
    return SHOP_SDK.load_data_model()


@pytest.fixture
def case_view(shop_model: dm.DataModel[dm.View]) -> dm.View:
    return next(v for v in shop_model.views if v.name == "Case")


@pytest.fixture
def command_config_view(shop_model: dm.DataModel[dm.View]) -> dm.View:
    return next(v for v in shop_model.views if v.name == "Command_Config")


@pytest.fixture(scope="session")
def market_models() -> list[dm.DataModel[dm.View]]:
    return MARKET_SDK.load_data_models()


@pytest.fixture(scope="session")
def cog_pool_model(market_models: list[dm.DataModel[dm.View]]) -> dm.DataModel[dm.View]:
    return next(m for m in market_models if m.name == "CogPool")


@pytest.fixture(scope="session")
def pygen_pool_model(market_models: list[dm.DataModel[dm.View]]) -> dm.DataModel[dm.View]:
    return next(m for m in market_models if m.name == "PygenPool")


@pytest.fixture(scope="session")
def date_transformation_pair_view(cog_pool_model) -> dm.View:
    return next(v for v in cog_pool_model.views if v.name == "DateTransformationPair")


@pytest.fixture(scope="session")
def bid_view(pygen_pool_model) -> dm.View:
    return next(v for v in pygen_pool_model.views if v.name == "Bid")


@pytest.fixture(scope="session")
def market_view(pygen_pool_model) -> dm.View:
    return next(v for v in pygen_pool_model.views if v.name == "Market")


@pytest.fixture(scope="session")
def apm_data_model() -> dm.DataModel[dm.View]:
    return APM_SDK.load_data_model()


@pytest.fixture(scope="session")
def pump_model() -> dm.DataModel[dm.View]:
    return PUMP_SDK.load_data_model()


@pytest.fixture(scope="session")
def scenario_instance_model() -> dm.DataModel[dm.View]:
    return SCENARIO_INSTANCE_SDK.load_data_model()


@pytest.fixture(scope="session")
def scenario_instance_view(scenario_instance_model: dm.DataModel[dm.View]) -> dm.View:
    return next(v for v in scenario_instance_model.views if v.name == "ScenarioInstance")


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
