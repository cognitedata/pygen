import json
from typing import Callable, TypeVar

import pytest

from cognite.pygen.utils.external_id_factories import create_incremental_factory, sha256_factory, uuid_factory
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


@pytest.mark.parametrize(
    "factory",
    [
        sha256_factory,
        create_incremental_factory(),
        uuid_factory,
    ],
)
def test_load_wells_from_json(
    factory: Callable[[type, dict], str],
) -> None:
    # Arrange
    expected_node_count = 145
    expected_edge_count = 105
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
        exclude = {"external_id", "space", "last_updated_time", "created_time", "deleted_time"}
        for windmill, json_item in zip(windmills, loaded_json):
            clear_nested_dependencies(windmill, json_item)
            if IS_PYDANTIC_V2:
                dumped_windmill = json.loads(
                    windmill.model_dump_json(by_alias=True, exclude=exclude, exclude_none=True)
                )
            else:
                dumped_windmill = json.loads(windmill.json(by_alias=True, exclude=exclude, exclude_none=True))
            assert dumped_windmill == json_item

        assert expected_node_count == len(created.nodes)
        assert expected_edge_count == len(created.edges)
    finally:
        DomainModelApply.external_id_factory = None
