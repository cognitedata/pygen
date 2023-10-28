import json
from pathlib import Path
from typing import Callable, TypeVar

import pytest

from cognite.pygen.utils.external_id_factories import create_incremental_factory, sha256_factory, uuid_factory
from tests.constants import IS_PYDANTIC_V2, OSDUWellsFiles

if IS_PYDANTIC_V2:
    from osdu_wells.client.data_classes import DomainModelApply, WellApply, WellboreApply, WellboreTrajectoryApply
else:
    from osdu_wells_pydantic_v1.client.data_classes import (
        DomainModelApply,
        WellApply,
        WellboreApply,
        WellboreTrajectoryApply,
    )

T_TypeNode = TypeVar("T_TypeNode", bound=DomainModelApply)


def clear_nested_dependencies(loaded: T_TypeNode, loaded_json: dict):
    # Clean nested dependencies for easy comparison

    if IS_PYDANTIC_V2:
        fields = loaded.model_fields.items()
    else:
        fields = loaded.__fields__.items()

    for name, field in fields:
        if not (
            isinstance(value := getattr(loaded, name), DomainModelApply)
            or (isinstance(value, list) and value and isinstance(value[0], DomainModelApply))
        ):
            continue
        setattr(loaded, name, None)
        loaded_json.pop(field.alias or name)


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
    try:
        DomainModelApply.external_id_factory = factory

        loaded_json = json.loads(raw_json)

        # Act
        if IS_PYDANTIC_V2:
            loaded = domain_cls.model_validate_json(raw_json)
        else:
            loaded = domain_cls.parse_raw(raw_json)
        created = loaded.to_instances_apply()

        # Assert
        exclude = {"external_id", "space", "last_updated_time", "created_time", "deleted_time"}
        clear_nested_dependencies(loaded, loaded_json)
        if IS_PYDANTIC_V2:
            assert json.loads(loaded.model_dump_json(by_alias=True, exclude=exclude, exclude_none=True)) == loaded_json
        else:
            assert json.loads(loaded.json(by_alias=True, exclude=exclude, exclude_none=True)) == loaded_json

        assert expected_node_count == len(created.nodes)
        assert expected_edge_count == len(created.edges)
    finally:
        DomainModelApply.external_id_factory = None
