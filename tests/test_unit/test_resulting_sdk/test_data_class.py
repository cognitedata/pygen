import json
from collections.abc import Callable
from typing import cast

import pytest
from cognite.client import data_modeling as dm
from omni import data_classes as dc
from pydantic import TypeAdapter
from windmill.data_classes import DomainModelWrite as WindmillDomainModelWrite
from windmill.data_classes import ResourcesWrite, WindmillWrite

from cognite.pygen.utils.external_id_factories import (
    ExternalIdFactory,
)
from tests.constants import OMNI_SDK, WindMillFiles
from tests.omni_constants import OmniClasses


def omni_nodes_with_view():
    nodes = OMNI_SDK.load_read_nodes(OMNI_SDK.data_model_ids[0])
    for node in nodes:
        node: dm.Node
        view_id = cast(dm.ViewId, next(iter(node.properties)))
        yield pytest.param(node, view_id, id=node.external_id)


class TestToFromInstances:
    @pytest.mark.parametrize("node, view_id", list(omni_nodes_with_view()))
    def test_from_to_instances(self, node: dm.Node, view_id: dm.ViewId, omni_data_classes: dict[str, OmniClasses]):
        if view_id.external_id in omni_data_classes:
            lookup_key = view_id.external_id
        elif f"{view_id.external_id}Node" in omni_data_classes:
            lookup_key = f"{view_id.external_id}Node"
        else:
            raise ValueError(f"No read class for {view_id}")

        read_cls = omni_data_classes[lookup_key].read

        domain_node = read_cls.from_instance(node)
        domain_write_node = domain_node.as_write()
        domain_write_node.to_pandas()

        resources = domain_write_node.to_instances_write()
        if not node.properties[view_id]:
            return
        assert len(resources.nodes) == 1

        node_write = node.as_write()
        # Bug in SDK that skips the type
        node_write.type = node.type
        assert node_write.dump() == resources.nodes[0].dump()

    @pytest.mark.parametrize("node, view_id", list(omni_nodes_with_view()))
    def test_writeable_to_instances(self, node: dm.Node, view_id: dm.ViewId, omni_data_classes: dict[str, OmniClasses]):
        if view_id.external_id in omni_data_classes:
            lookup_key = view_id.external_id
        elif f"{view_id.external_id}Node" in omni_data_classes:
            lookup_key = f"{view_id.external_id}Node"
        else:
            raise ValueError(f"No view class for {view_id}")
        view = omni_data_classes[lookup_key].view
        if any(prop for prop in view.properties.values() if isinstance(prop, dm.MappedProperty) and prop.default_value):
            # Mapped properties with default values will not return the same node
            # This is intentional, as write note should give hints to the user
            return

        if omni_data_classes[lookup_key].write is not None:
            # If there is no write class, then there is nothing to test
            return

        read_cls = omni_data_classes[lookup_key].write
        node_write = node.as_write()
        # Bug in SDK that skips the type
        node_write.type = node.type
        domain_write_node = read_cls.from_instance(node_write)
        domain_write_node.to_pandas()

        resources = domain_write_node.to_instances_write()
        if not node.properties[view_id]:
            return
        assert len(resources.nodes) == 1

        assert node_write.dump() == resources.nodes[0].dump()


class TestToInstancesWrite:
    def test_single_edge_to_instances_write(self):
        # Arrange
        connection = dc.ConnectionItemDWrite(
            external_id="test_single_edge_to_instances_write",
            name="connectionD",
            outwards_single=dc.ConnectionItemEWrite(
                name="connectionE",
                external_id="test_single_edge_to_instances_write:connectionE",
            ),
        )

        # Act
        resources = connection.to_instances_write()

        # Assert
        assert len(resources.nodes) == 2
        assert len(resources.edges) == 1

    def test_create_connection_with_direct_relation_and_tuple(self) -> None:
        connection = dc.ConnectionItemAWrite(
            external_id="test_create_connection_with_direct_relation_and_tuple",
            name="connectionA",
            other_direct=("my_space", "my_external_id"),
            outwards=[dm.DirectRelationReference(space="my_space", external_id="my_external_id2")],
            self_direct=dm.NodeId(space="my_space", external_id="my_external_id3"),
        )

        assert connection.other_direct == dm.NodeId(space="my_space", external_id="my_external_id")
        assert connection.outwards == [dm.NodeId(space="my_space", external_id="my_external_id2")]
        assert connection.self_direct == dm.NodeId(space="my_space", external_id="my_external_id3")


@pytest.mark.parametrize(
    "factory, expected_node_count, expected_edge_count",
    [
        # There are none unique sensor positions in the windmill data
        # so hashing it will lead to fewer nodes
        (
            ExternalIdFactory.create_external_id_factory(suffix_ext_id_factory=ExternalIdFactory.sha256_factory()),
            135,
            105,
        ),
        (
            ExternalIdFactory.create_external_id_factory(suffix_ext_id_factory=ExternalIdFactory.incremental_factory()),
            145,
            105,
        ),
        (
            ExternalIdFactory.create_external_id_factory(suffix_ext_id_factory=ExternalIdFactory.uuid_factory()),
            145,
            105,
        ),
        (ExternalIdFactory.sha256_factory().short, 135, 105),
        (ExternalIdFactory.uuid_factory().short, 145, 105),
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
        WindmillDomainModelWrite.external_id_factory = factory

        loaded_json = json.loads(raw_json)

        # Act
        windmills = TypeAdapter(list[WindmillWrite]).validate_json(raw_json)

        created = ResourcesWrite()
        for item in windmills:
            created.extend(item.to_instances_write())

        # Assert
        exclude = {"external_id", "space", "data_record", "externalId"}
        for windmill, json_item in zip(windmills, loaded_json, strict=False):
            dumped_windmill = json.loads(
                windmill.model_dump_json(by_alias=True, exclude=exclude, exclude_none=True, exclude_unset=True)
            )

            # The exclude=True is not recursive in pydantic, so we need to do it manually
            _recursive_exclude(dumped_windmill, exclude)
            assert dumped_windmill == json_item

        assert len(created.nodes) == expected_node_count
        assert len(created.edges) == expected_edge_count
    finally:
        WindmillDomainModelWrite.external_id_factory = None


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
