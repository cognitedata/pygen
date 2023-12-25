import json
from typing import Callable, TypeVar

import pytest

from cognite.pygen.utils.external_id_factories import (
    create_incremental_factory,
    create_sha256_factory,
    create_uuid_factory,
    sha256_factory,
    uuid_factory,
)
from tests.constants import IS_PYDANTIC_V2, WindMillFiles

if IS_PYDANTIC_V2:
    from pydantic import TypeAdapter
    from windmill.client.data_classes import DomainModelApply, ResourcesApply, WindmillApply
else:
    from pydantic import parse_obj_as

    raise NotImplementedError

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


def recursive_exclude(d: dict, exclude: set[str]) -> None:
    for key in list(d.keys()):
        value = d[key]
        if isinstance(value, dict):
            recursive_exclude(value, exclude)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    recursive_exclude(item, exclude)
        elif key in exclude:
            d.pop(key)


@pytest.mark.parametrize(
    "factory, expected_node_count, expected_edge_count",
    [
        # There are none unique sensor positions in the windmill data
        # so hashing it will lead to fewer nodes
        (sha256_factory, 135, 105),
        (create_incremental_factory(), 145, 105),
        (uuid_factory, 145, 105),
        (create_sha256_factory(True), 135, 105),
        (create_uuid_factory(True), 145, 105),
    ],
)
def test_load_wells_from_json(
    factory: Callable[[type, dict], str],
    expected_node_count: int,
    expected_edge_count: int,
) -> None:
    # Arrange
    raw_json = WindMillFiles.Data.wind_mill_json.read_text()
    try:
        DomainModelApply.external_id_factory = factory

        loaded_json = json.loads(raw_json)

        # Act
        if IS_PYDANTIC_V2:
            windmills = TypeAdapter(list[WindmillApply]).validate_json(raw_json)
        else:
            windmills = parse_obj_as(list[WindmillApply], raw_json)
        created = ResourcesApply()
        for item in windmills:
            created.extend(item.to_instances_apply())

        # Assert
        exclude = {"external_id", "space"}
        for windmill, json_item in zip(windmills, loaded_json):
            if IS_PYDANTIC_V2:
                dumped_windmill = json.loads(
                    windmill.model_dump_json(by_alias=True, exclude=exclude, exclude_none=True)
                )
            else:
                dumped_windmill = json.loads(windmill.json(by_alias=True, exclude=exclude, exclude_none=True))
            # The exclude=True is not recursive in pydantic, so we need to do it manually
            recursive_exclude(dumped_windmill, exclude)
            assert dumped_windmill == json_item

        assert len(created.nodes) == expected_node_count
        assert len(created.edges) == expected_edge_count
    finally:
        DomainModelApply.external_id_factory = None
