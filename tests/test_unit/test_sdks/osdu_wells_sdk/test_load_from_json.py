import json
from pathlib import Path
from typing import Callable

import pytest

from cognite.pygen.utils.external_id_factories import create_incremental_factory, sha256_factory, uuid_factory
from tests.constants import IS_PYDANTIC_V1, OSDUWellsFiles

if not IS_PYDANTIC_V1:
    from osdu_wells.client.data_classes import DomainModelApply, WellApply, WellboreApply, WellboreTrajectoryApply
else:
    raise NotImplementedError


@pytest.mark.parametrize(
    "example_file, domain_cls, factory, expected_node_count, expected_edge_count",
    [
        (OSDUWellsFiles.Data.wellbore_trajectory, WellboreTrajectoryApply, sha256_factory, 32, 19),
        (OSDUWellsFiles.Data.wellbore, WellboreApply, create_incremental_factory(), 47, 26),
        (OSDUWellsFiles.Data.well, WellApply, uuid_factory, 30, 19),
    ],
)
def test_load_wells_from_json(
    example_file: Path,
    domain_cls: type[DomainModelApply],
    factory: Callable[[type, dict], str],
    expected_node_count: int,
    expected_edge_count: int,
) -> None:
    # Arrange
    raw_json = example_file.read_text()
    DomainModelApply.external_id_factory = factory
    loaded_json = json.loads(raw_json)

    # Act
    loaded = domain_cls.model_validate_json(raw_json)
    created = loaded.to_instances_apply()

    # Assert
    exclude = {"external_id", "space", "last_updated_time", "created_time", "deleted_time"}
    if IS_PYDANTIC_V1:
        assert json.loads(loaded.json(by_alias=True, exclude=exclude, exclude_none=True)) == json.loads(raw_json)
    else:
        # Clean nested dependencies for easy comparison
        for name, field in loaded.model_fields.items():
            if not (
                isinstance(value := getattr(loaded, name), DomainModelApply)
                or (isinstance(value, list) and value and isinstance(value[0], DomainModelApply))
            ):
                continue
            setattr(loaded, name, None)
            loaded_json.pop(field.alias or name)

        assert json.loads(loaded.model_dump_json(by_alias=True, exclude=exclude, exclude_none=True)) == loaded_json
    assert expected_node_count == len(created.nodes)
    assert expected_edge_count == len(created.edges)
