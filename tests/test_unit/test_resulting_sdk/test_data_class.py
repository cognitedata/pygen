import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import cast

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    FileMetadata,
    FileMetadataWrite,
    Sequence,
    SequenceColumn,
    SequenceWrite,
    TimeSeries,
    TimeSeriesWrite,
)
from cognite_core.data_classes import CogniteFileWrite
from omni import data_classes as dc
from pydantic import TypeAdapter
from wind_turbine.data_classes import DomainModelWrite as WindDomainModelWrite
from wind_turbine.data_classes import ResourcesWrite, WindTurbineWrite

from cognite.pygen.utils.external_id_factories import (
    ExternalIdFactory,
)
from tests.constants import JSON_DIR, OMNI_SDK
from tests.omni_constants import OmniClasses


def omni_nodes_with_view():
    nodes = OMNI_SDK.load_read_nodes(OMNI_SDK.data_model_ids[0])
    for node in nodes:
        if node.external_id.startswith("Implementation1NonWriteable"):
            # Hacky way to skip non-writeable nodes
            continue
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
        # All subclasses fo domain_node has as-write
        # but they have different return types so adding an abstract method
        # would mean use of generics.
        domain_write_node = domain_node.as_write()  # type: ignore[attr-defined]
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
        assert read_cls is not None
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
            # Pydantic converts these to the correct type
            # We need to turn-off the type checker for this line
            other_direct=("my_space", "my_external_id"),  # type: ignore[arg-type]
            outwards=[dm.DirectRelationReference(space="my_space", external_id="my_external_id2")],  # type: ignore[list-item]
            self_direct=dm.NodeId(space="my_space", external_id="my_external_id3"),
        )

        assert connection.other_direct == dm.NodeId(space="my_space", external_id="my_external_id")
        assert connection.outwards == [dm.NodeId(space="my_space", external_id="my_external_id2")]
        assert connection.self_direct == dm.NodeId(space="my_space", external_id="my_external_id3")


@pytest.mark.parametrize(
    "factory, expected_node_count, expected_edge_count",
    [
        # There are none unique sensor positions in the windturbine data
        # so hashing it will lead to fewer nodes
        (
            ExternalIdFactory.create_external_id_factory(suffix_ext_id_factory=ExternalIdFactory.sha256_factory()),
            4,
            0,
        ),
        (
            ExternalIdFactory.create_external_id_factory(suffix_ext_id_factory=ExternalIdFactory.incremental_factory()),
            4,
            0,
        ),
        (
            ExternalIdFactory.create_external_id_factory(suffix_ext_id_factory=ExternalIdFactory.uuid_factory()),
            4,
            0,
        ),
        (ExternalIdFactory.sha256_factory().short, 4, 0),
        (ExternalIdFactory.uuid_factory().short, 4, 0),
    ],
)
def test_load_windturbines_from_json(
    factory: Callable[[type, dict], str],
    expected_node_count: int,
    expected_edge_count: int,
) -> None:
    # Arrange
    raw_json = (JSON_DIR / "turbine.json").read_text()
    try:
        WindDomainModelWrite.external_id_factory = factory

        loaded_json = json.loads(raw_json)

        # Act
        turbines = TypeAdapter(list[WindTurbineWrite]).validate_json(raw_json)

        created = ResourcesWrite()
        for item in turbines:
            created.extend(item.to_instances_write())

        # Assert
        exclude = {"external_id", "space", "data_record", "externalId"}
        for wind_turbine, json_item in zip(turbines, loaded_json, strict=False):
            dumped_turbine = json.loads(
                wind_turbine.model_dump_json(by_alias=True, exclude=exclude, exclude_none=True, exclude_unset=True)
            )

            # The exclude=True is not recursive in pydantic, so we need to do it manually
            _recursive_exclude(dumped_turbine, exclude)
            assert dumped_turbine == json_item

        assert len(created.nodes) == expected_node_count
        assert len(created.edges) == expected_edge_count
    finally:
        WindDomainModelWrite.external_id_factory = None


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


def test_cognite_file_as_nodes() -> None:
    # CogniteFile and CogniteAsset has read-only properties that can cause issus.
    obj = CogniteFileWrite(
        external_id="test_cognite_file_as_nodes", source_id="source_id", mime_type="application/json", directory="/tmp"
    )

    resources = obj.to_instances_write()

    assert len(resources.nodes) == 1


class TestAsWrite:
    def test_as_write_with_extra_arg(self) -> None:
        now = datetime.now(timezone.utc)
        read_cls = dc.PrimitiveNullable(  # type: ignore[call-arg]
            space="my_space",
            external_id="my_external_id",
            data_record=dc.DataRecord(version=1, last_updated_time=now, created_time=now),
            text="Some text",
            extra="My New Property",
        )

        write_cls = read_cls.as_write()
        assert write_cls.model_dump(exclude_unset=True, by_alias=True) == {
            "space": "my_space",
            "externalId": "my_external_id",
            "data_record": {"existing_version": 1},
            "text": "Some text",
        }

    def test_as_write_with_cdf_external_references(self) -> None:
        now = datetime.now(timezone.utc)
        read_instance = dc.CDFExternalReferences(
            external_id="my_external_id",
            data_record=dc.DataRecord(
                version=1,
                last_updated_time=now,
                created_time=now,
                deleted_time=None,
            ),
            node_type=None,
            file=FileMetadata(
                id=123456789,
                external_id="my_file_external_id",
                source="my_source_id",
                mime_type="application/json",
                directory="/tmp",
                name="my_file_external_id",
            ),
            timeseries=TimeSeries(
                id=987654321,
                external_id="my_timeseries_external_id",
                name="my_timeseries_external_id",
                is_step=False,
                is_string=False,
            ),
            sequence=Sequence(
                id=1122334455,
                external_id="my_sequence_external_id",
                name="my_sequence_external_id",
                columns=[
                    SequenceColumn(
                        external_id="column1",
                        name="column1",
                        value_type="String",
                    )
                ],
            ),
        )

        write = read_instance.as_write()

        assert isinstance(write, dc.CDFExternalReferencesWrite)
        assert isinstance(write.file, FileMetadataWrite)
        assert isinstance(write.sequence, SequenceWrite)
        assert isinstance(write.timeseries, TimeSeriesWrite)
