import pytest
from cognite.client import data_modeling as dm
from yaml import safe_load

from tests.constants import DataModels


@pytest.fixture(scope="session")
def movie_model() -> dm.DataModel:
    with DataModels.movie_model.open("r") as f:
        raw = safe_load(f)
    return dm.DataModel.load(raw)


@pytest.fixture(scope="session")
def person_view(movie_model: dm.DataModel) -> dm.View:
    return next(v for v in movie_model.views if v.default == "Person")


@pytest.fixture(scope="session")
def actor_view(movie_model: dm.DataModel) -> dm.View:
    return next(v for v in movie_model.views if v.default == "Actor")


@pytest.fixture(scope="session")
def movie_views(movie_model: dm.DataModel) -> dm.ViewList:
    return dm.ViewList(movie_model.views)


@pytest.fixture
def shop_model() -> dm.DataModel:
    with DataModels.shop_model.open("r") as f:
        raw = safe_load(f)
    return dm.DataModel.load(raw)


@pytest.fixture
def case_view(shop_model) -> dm.View:
    return next(v for v in shop_model.views if v.default == "Case")


@pytest.fixture
def command_config_view(shop_model) -> dm.View:
    return next(v for v in shop_model.views if v.default == "Command_Config")
