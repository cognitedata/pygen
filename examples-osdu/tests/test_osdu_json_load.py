from pathlib import Path

import pytest
from osdu import osdu
from osdu.data_classes import WellApply, WellboreApply, WellboreTrajectoryApply
from rich import print

from tests.constants import ROOT_DIRECTORY


def osdu_well():
    yield pytest.param(
        config := ROOT_DIRECTORY / "../config/osdu-master-data-well-dumps/osdu-master-well:1.3.0.patched.json",
        id=config.name,
    )
    # more can be yielded here


def osdu_wellbore():
    yield pytest.param(
        config := ROOT_DIRECTORY / "../config/osdu-master-data-wellbore-dumps/osdu-master-wellbore:1.5.0.patched.json",
        id=config.name,
    )


def osdu_trajectory():
    yield pytest.param(
        config := ROOT_DIRECTORY / "../config/osdu-work-product-component-wellboretrajectory-dumps/"
        "osdu-work-product-component-wellboretrajectory:1.3.0.patched.json",
        id=config.name,
    )


@pytest.fixture
def osdu_client():
    # create a osdu client
    return osdu.from_toml(ROOT_DIRECTORY / "../config/osdu_client_env.toml")


@pytest.mark.parametrize("example_file", osdu_trajectory())
def test_load_trajectory(example_file: Path, osdu_client, initialize_pygen_client):
    # load the wellbore-trajectory
    with example_file.open() as f:
        wellbore_trajectory = WellboreTrajectoryApply.model_validate_json(f.read())
        print("pygen WellboreTrajectoryApply: ")
        print(wellbore_trajectory.model_dump(by_alias=True))
        assert wellbore_trajectory
        create = osdu_client.wellbore_trajectory.apply(wellbore_trajectory)
        print(f"{len(create.nodes)=}, {len(create.edges)=}")
        print(create.nodes)
        print(create.edges)


@pytest.mark.parametrize("example_file", osdu_wellbore())
def test_load_wellbore(example_file: Path, osdu_client, initialize_pygen_client):
    # load the wellbore
    with example_file.open() as f:
        wellbore = WellboreApply.model_validate_json(f.read())
        print("pygen WellboreApply: ")
        print(wellbore.model_dump(by_alias=True))
        assert wellbore
        create = osdu_client.wellbore.apply(wellbore)
        print(f"{len(create.nodes)=}, {len(create.edges)=}")
        print(create.nodes)
        print(create.edges)


@pytest.mark.parametrize("example_file", osdu_well())
def test_load_well(example_file: Path, osdu_client, initialize_pygen_client):
    # load the well
    with example_file.open() as f:
        well = WellApply.model_validate_json(f.read())
        print("pygen WellApply: ")
        print(well.model_dump(by_alias=True))
        assert well

        create = osdu_client.well.apply(well)
        print(f"{len(create.nodes)=}, {len(create.edges)=}")
        print(create.nodes)
        print(create.edges)
