import json
from typing import Callable, cast

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen.utils.external_id_factories import (
    create_incremental_factory,
    create_sha256_factory,
    create_uuid_factory,
    sha256_factory,
    uuid_factory,
)
from tests.constants import IS_PYDANTIC_V2, OMNI_SDK, WindMillFiles
from tests.omni_constants import OmniClasses

if IS_PYDANTIC_V2:
    from pydantic import TypeAdapter
    from windmill.client.data_classes import DomainModelApply as WindmillDomainModelApply
    from windmill.client.data_classes import ResourcesApply, WindmillApply
else:
    from pydantic import parse_obj_as
    from windmill_pydantic_v1.client.data_classes import (
        DomainModelApply as WindmillDomainModelApply,
    )
    from windmill_pydantic_v1.client.data_classes import (
        ResourcesApply,
        WindmillApply,
    )


def omni_nodes_with_view():
    nodes = OMNI_SDK.load_read_nodes(OMNI_SDK.data_model_ids[0])
    for node in nodes:
        node: dm.Node
        view_id = cast(dm.ViewId, next(iter(node.properties)))
        yield pytest.param(node, view_id, id=node.external_id)


class TestToFromInstances:
    @pytest.mark.parametrize("node, view_id", list(omni_nodes_with_view()))
    def test_from_to_instances(self, node: dm.Node, view_id: dm.ViewId, omni_data_classes: dict[str, OmniClasses]):
        read_cls = omni_data_classes[view_id.external_id].read

        domain_node = read_cls.from_instance(node)
        domain_apply_node = domain_node.as_apply()
        domain_apply_node.to_pandas()

        resources = domain_apply_node.to_instances_apply()
        if not node.properties[view_id]:
            return
        assert len(resources.nodes) == 1

        node_apply = node.as_apply(None, None)
        # Bug in SDK that skips the type
        node_apply.type = node.type
        assert node_apply.dump() == resources.nodes[0].dump()

    @pytest.mark.parametrize("node, view_id", list(omni_nodes_with_view()))
    def test_writeable_to_instances(self, node: dm.Node, view_id: dm.ViewId, omni_data_classes: dict[str, OmniClasses]):
        view = omni_data_classes[view_id.external_id].view
        if any(prop for prop in view.properties.values() if isinstance(prop, dm.MappedProperty) and prop.default_value):
            # Mapped properties with default values will not return the same node
            # This is intentional, as write note should give hints to the user
            return

        if omni_data_classes[view_id.external_id].write is not None:
            # If there is no write class, then there is nothing to test
            return

        read_cls = omni_data_classes[view_id.external_id].write
        node_apply = node.as_apply(None, None)
        # Bug in SDK that skips the type
        node_apply.type = node.type
        domain_apply_node = read_cls.from_instance(node_apply)
        domain_apply_node.to_pandas()

        resources = domain_apply_node.to_instances_apply()
        if not node.properties[view_id]:
            return
        assert len(resources.nodes) == 1

        assert node_apply.dump() == resources.nodes[0].dump()


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
def test_load_windmills_from_json(
    factory: Callable[[type, dict], str],
    expected_node_count: int,
    expected_edge_count: int,
) -> None:
    # Arrange
    raw_json = WindMillFiles.Data.wind_mill_json.read_text()
    try:
        WindmillDomainModelApply.external_id_factory = factory

        loaded_json = json.loads(raw_json)

        # Act
        if IS_PYDANTIC_V2:
            windmills = TypeAdapter(list[WindmillApply]).validate_json(raw_json)
        else:
            windmills = parse_obj_as(list[WindmillApply], loaded_json)
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
            _recursive_exclude(dumped_windmill, exclude)
            assert dumped_windmill == json_item

        assert len(created.nodes) == expected_node_count
        assert len(created.edges) == expected_edge_count
    finally:
        WindmillDomainModelApply.external_id_factory = None


def _recursive_exclude(d: dict, exclude: set[str]) -> None:
    for key in list(d.keys()):
        value = d[key]
        if isinstance(value, dict):
            _recursive_exclude(value, exclude)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _recursive_exclude(item, exclude)
        elif key in exclude:
            d.pop(key)
