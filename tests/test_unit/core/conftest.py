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
    return next(v for v in movie_model.views if v.name == "Person")


@pytest.fixture
def actor_view() -> dm.View:
    return dm.View.load(
        {
            "space": "IntegrationTestsImmutable",
            "external_id": "Actor",
            "name": "Actor",
            "implements": [
                {"space": "IntegrationTestsImmutable", "external_id": "Role", "version": "2", "type": "view"}
            ],
            "version": "2",
            "writable": True,
            "used_for": "node",
            "is_global": False,
            "properties": {
                "wonOscar": {
                    "container": {"space": "IntegrationTestsImmutable", "external_id": "Role", "type": "container"},
                    "container_property_identifier": "wonOscar",
                    "name": "wonOscar",
                    "description": None,
                    "type": {"list": False, "type": "boolean"},
                    "nullable": True,
                    "auto_increment": False,
                    "default_value": None,
                },
                "person": {
                    "container": {"space": "IntegrationTestsImmutable", "external_id": "Role", "type": "container"},
                    "container_property_identifier": "person",
                    "name": "person",
                    "description": None,
                    "type": {
                        "source": {
                            "space": "IntegrationTestsImmutable",
                            "external_id": "Person",
                            "version": "2",
                            "type": "view",
                        },
                        "type": "direct",
                    },
                    "nullable": True,
                    "auto_increment": False,
                    "default_value": None,
                },
                "movies": {
                    "type": {"space": "IntegrationTestsImmutable", "external_id": "Role.movies"},
                    "source": {
                        "space": "IntegrationTestsImmutable",
                        "external_id": "Movie",
                        "version": "2",
                        "type": "view",
                    },
                    "name": "movies",
                    "description": None,
                    "edge_source": None,
                    "direction": "outwards",
                },
                "nomination": {
                    "type": {"space": "IntegrationTestsImmutable", "external_id": "Role.nomination"},
                    "source": {
                        "space": "IntegrationTestsImmutable",
                        "external_id": "Nomination",
                        "version": "2",
                        "type": "view",
                    },
                    "name": "nomination",
                    "description": None,
                    "edge_source": None,
                    "direction": "outwards",
                },
            },
            "last_updated_time": 1684079343668,
            "created_time": 1684079343668,
        }
    )
