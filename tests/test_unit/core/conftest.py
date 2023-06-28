import pickle

import pytest
from cognite.client import data_modeling as dm

from tests.constants import DataModels


@pytest.fixture(scope="session")
def movie_model() -> dm.DataModel:
    # with DataModels.movie_model.open("r") as f:
    #     raw = safe_load(f)
    # return dm.DataModel.load(raw)
    with DataModels.movie_model_pickle.open("rb") as f:
        return pickle.load(f)[0]


@pytest.fixture(scope="session")
def person_view(movie_model: dm.DataModel) -> dm.View:
    return next(v for v in movie_model.views if v.name == "Person")


@pytest.fixture
def actor_view(movie_model: dm.DataModel) -> dm.View:
    return next(v for v in movie_model.views if v.name == "Actor")
