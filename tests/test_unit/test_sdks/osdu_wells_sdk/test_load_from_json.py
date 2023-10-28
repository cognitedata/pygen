from pathlib import Path
from typing import Callable

import pytest

from cognite.pygen.utils.external_id_factories import uuid_factory
from tests.constants import IS_PYDANTIC_V1, OSDUWellsFiles

if not IS_PYDANTIC_V1:
    from osdu_wells.client.data_classes import DomainModelApply, WellApply
else:
    raise NotImplementedError


@pytest.mark.parametrize(
    "example_file, domain_cls, factory, expected_node_count, expected_edge_count",
    [
        (OSDUWellsFiles.Data.well, WellApply, uuid_factory, 1, 1),
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

    # Act
    loaded = domain_cls.model_validate_json(raw_json)
    created = loaded.to_instances_apply()

    # Assert
    if IS_PYDANTIC_V1:
        assert loaded.json(by_alias=True) == raw_json
    else:
        assert loaded.model_dump_json(by_alias=True) == raw_json
    assert len(created.nodes) == expected_node_count
    assert len(created.edges) == expected_edge_count
