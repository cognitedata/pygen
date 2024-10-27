"""This module contains the mock generator for the pygen package. It can be used to generate mock nodes,
edges, timeseries, sequences, and files for a given data model/views.
"""

from __future__ import annotations

import hashlib
import json
import random
import string
import typing
import warnings
from collections import UserList, defaultdict
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from graphlib import TopologicalSorter
from pathlib import Path
from random import choice, choices, randint, uniform
from typing import Generic, Literal, cast

import numpy as np
import pandas as pd
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    FileMetadata,
    FileMetadataList,
    Sequence,
    SequenceColumn,
    SequenceList,
    TimeSeries,
    TimeSeriesList,
)
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from cognite.client.data_classes.data_modeling.data_types import Enum, ListablePropertyType
from cognite.client.data_classes.data_modeling.views import (
    EdgeConnection,
    MultiEdgeConnection,
    ReverseDirectRelation,
    SingleEdgeConnection,
)
from cognite.client.exceptions import CogniteNotFoundError

from cognite.pygen._version import __version__
from cognite.pygen.exceptions import PygenImportError
from cognite.pygen.utils.cdf import _find_first_node_type

DataType = typing.Union[int, float, bool, str, dict, None]
ListAbleDataType = typing.Union[
    int,
    float,
    bool,
    str,
    dict,
    list[int],
    list[float],
    list[bool],
    list[str],
    list[dict],
    None,
]
ResourceType = Literal["node", "edge", "timeseries", "sequence", "file"]
_ResourceTypes = set(typing.get_args(ResourceType))

_READONLY_PROPERTIES: dict[dm.ViewId, set[str]] = {
    dm.ViewId("cdf_cdm", "CogniteAsset", "v1"): {"root", "path", "pathLastUpdatedTime"}
}


class MockGenerator:
    """Mock generator for the pygen package. It can be used to generate mock nodes, edges, timeseries,
    sequences, and files for a given data model/views.

    Args:
        views (List[View]): The views to generate mock data for.
        instance_space (str): The space to use for the generated nodes and edges.
        view_configs (dict[ViewId, ViewMockConfig]): Configuration for how to generate mock data for the different
            views. The keys are the view ids, and the values are the configuration for the view.
        default_config (ViewMockConfig): Default configuration for how to generate mock data for the different
            views. Set to 'faker' to use the Python package faker to generate mock data.
        data_set_id (int): The data set id to use for TimeSeries, Sequences, and FileMetadata.
        seed (int): The seed to use for the random number generator. If provided, it is used to reset the seed for
            each view to ensure reproducible results.
        skip_interfaces (bool): Whether to skip interfaces when generating mock data. Defaults to False.
    """

    def __init__(
        self,
        views: typing.Sequence[dm.View],
        instance_space: str,
        view_configs: dict[dm.ViewId, ViewMockConfig] | None = None,
        default_config: ViewMockConfig | Literal["faker"] | None = None,
        data_set_id: int | None = None,
        seed: int | None = None,
        skip_interfaces: bool = False,
    ):
        self._views = dm.ViewList(views)
        self._instance_space = instance_space
        self._view_configs = view_configs or {}
        if default_config == "faker":
            self._default_config = _create_faker_config()
        else:
            self._default_config = default_config or ViewMockConfig()
        self._data_set_id = data_set_id
        self._seed = seed
        self._skip_interfaces = skip_interfaces
        self._interfaces: set[dm.ViewId] = set()

    def __str__(self):
        args = [
            f"view_count={len(self._views)}",
            f"instance_space={self._instance_space}",
        ]
        if self._view_configs:
            args.append(f"custom_config_cont={len(self._view_configs)}")
        if self._default_config == ViewMockConfig():
            args.append("default_config=True")
        else:
            args.append("default_config=False")
        if self._data_set_id:
            args.append(f"data_set_id={self._data_set_id}")
        if self._seed:
            args.append(f"seed={self._seed}")

        return f"MockGenerator({', '.join(args)})"

    def _repr_html_(self) -> str:
        return str(self)

    @classmethod
    def from_data_model(
        cls,
        data_model_id: DataModelIdentifier,
        instance_space: str,
        client: CogniteClient,
        data_set_id: int | None = None,
        seed: int | None = None,
    ) -> MockGenerator:
        """Creates a MockGenerator from a data model.

        Args:
            data_model_id: Identifier of the data model to generate mock data for.
            instance_space: The space to use for the generated nodes and edges.
            client: An instance of the CogniteClient class.
            data_set_id: The data set id to use for TimeSeries, Sequences, and FileMetadata.
            seed: The seed to use for the random number generator.
            default_config:

        Returns:
            MockGenerator: The mock generator.

        """
        with _log_pygen_mock_call(client) as client:
            data_model = client.data_modeling.data_models.retrieve(
                ids=data_model_id,
                inline_views=True,
            ).latest_version()

        return cls(
            views=data_model.views,
            instance_space=instance_space,
            data_set_id=data_set_id,
            seed=seed,
        )

    def generate_mock_data(
        self, node_count: int = 5, max_edge_per_type: int = 5, null_values: float = 0.25
    ) -> MockData:
        """Generates mock data for the given data model/views.


        Args:
            node_count: The number of nodes to generate for each view.
            max_edge_per_type: The maximum number of edges to generate for each edge type.
            null_values: The probability of generating a null value for a nullable properties.

        Returns:
            MockData: The generated mock data.
        """
        self._interfaces = {interface for view in self._views for interface in view.implements or []}
        mock_data = MockData()
        for connected_views in _connected_views(self._views):
            data = self._generate_views_mock_data(connected_views, node_count, max_edge_per_type, null_values)
            mock_data.extend(data)
        return mock_data

    def _generate_views_mock_data(self, views: list[dm.View], node_count, max_edge_per_type, null_values) -> MockData:
        outputs = self._generate_mock_nodes(views, node_count, null_values)
        self._generate_mock_connections(views, outputs, max_edge_per_type, null_values)
        return MockData(outputs.values())

    def _generate_mock_nodes(
        self, views: list[dm.View], default_node_count: int, default_nullable_fraction: float
    ) -> dict[dm.ViewId, ViewMockData]:
        output: dict[dm.ViewId, ViewMockData] = {}
        for view in sorted(views, key=lambda v: v.as_id().as_tuple()):
            if self._skip_interfaces and view.as_id() in self._interfaces:
                continue
            if view.used_for == "edge":
                continue
            mapped_properties = {
                name: prop
                for name, prop in view.properties.items()
                if isinstance(prop, dm.MappedProperty) and not isinstance(prop.type, dm.DirectRelation)
            }

            node_type = _find_first_node_type(view.filter)
            view_id = view.as_id()
            if self._seed:
                self._reset_seed(view_id)

            config = self._view_configs.get(view_id, self._default_config)
            properties, external = self._generate_mock_values(
                mapped_properties,
                config,
                view.as_id(),
                default_node_count,
                default_nullable_fraction,
            )
            node_ids = config.node_id_generator(view_id, config.node_count or default_node_count)

            nodes = [
                dm.NodeApply(
                    space=self._instance_space,
                    external_id=node_id,
                    type=node_type,
                    sources=(
                        [
                            dm.NodeOrEdgeData(
                                source=view.as_id(),
                                properties=dict(zip(properties.keys(), props, strict=False)),
                            )
                        ]
                        if props
                        else None
                    ),
                )
                for node_id, *props in zip(node_ids, *properties.values(), strict=False)
            ]
            output[view.as_id()] = ViewMockData(
                view.as_id(),
                instance_space=self._instance_space,
                is_writeable=view.writable,
                node=dm.NodeApplyList(nodes),
                timeseries=TimeSeriesList(external.timeseries),
                sequence=SequenceList(external.sequence),
                file=FileMetadataList(external.file),
            )
        return output

    def _generate_mock_connections(
        self,
        views: list[dm.View],
        outputs: dict[dm.ViewId, ViewMockData],
        default_max_edge_count: int,
        default_nullable_fraction: float,
    ) -> None:
        leaf_children_by_parent = self._to_leaf_children_by_parent(views)
        for view in sorted(views, key=lambda v: v.as_id().as_tuple()):
            if self._skip_interfaces and view.as_id() in self._interfaces:
                continue
            connection_properties = {
                name: prop
                for name, prop in view.properties.items()
                if (isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation))
                or isinstance(prop, dm.ConnectionDefinition)
            }
            if not connection_properties:
                continue
            view_id = view.as_id()

            if self._seed:
                self._reset_seed(view_id)

            config = self._view_configs.get(view_id, self._default_config)
            for this_node in outputs[view_id].node:
                for property_name, connection in connection_properties.items():
                    if (
                        isinstance(connection, MultiEdgeConnection | dm.MappedProperty)
                        and connection.source is not None
                        and connection.source not in outputs
                        and connection.source not in leaf_children_by_parent
                    ):
                        warnings.warn(
                            f"{view_id} property {property_name!r} points to a view {connection.source} "
                            f"which is not in the data model. Skipping connection generation.",
                            stacklevel=2,
                        )
                        continue

                    if view_id in _READONLY_PROPERTIES and property_name in _READONLY_PROPERTIES[view_id]:
                        continue

                    if isinstance(connection, EdgeConnection):
                        other_nodes = self._get_other_nodes(connection.source, outputs, leaf_children_by_parent)
                        if isinstance(connection, SingleEdgeConnection):
                            max_edge_count = 1
                        else:  # MultiEdgeConnection
                            max_edge_count = config.max_edge_per_type or default_max_edge_count
                        max_edge_count = min(max_edge_count, len(other_nodes))
                        edges = self._create_edges(connection, this_node.as_id(), other_nodes, max_edge_count)
                        outputs[view_id].edge.extend(edges)
                    elif isinstance(connection, dm.MappedProperty) and isinstance(connection.type, dm.DirectRelation):
                        if not connection.source:
                            warnings.warn(
                                f"View {view_id}: DirectRelation {property_name} is missing source, "
                                "do not know the target view the direct relation points to",
                                stacklevel=2,
                            )
                            continue
                        other_nodes = self._get_other_nodes(connection.source, outputs, leaf_children_by_parent)

                        # If the connection is nullable, we randomly decide if we should create the relation
                        create_relation = not connection.nullable or random.random() < (
                            1 - (config.null_values or default_nullable_fraction)
                        )
                        if not (create_relation and other_nodes):
                            continue
                        if connection.type.is_list:
                            max_edge_count = config.max_edge_per_type or default_max_edge_count
                        else:
                            max_edge_count = 1
                        other_nodes = random.sample(other_nodes, k=randint(1, max_edge_count))
                        values = [
                            {"space": other_node.space, "externalId": other_node.external_id}
                            for other_node in other_nodes
                        ]
                        value: dict | list[dict] = values if connection.type.is_list else values[0]
                        self._set_direct_relation_property(this_node, view_id, property_name, value)
                    elif isinstance(connection, ReverseDirectRelation):
                        continue
                    else:
                        warnings.warn(
                            f"View {view_id}: Connection {type(connection)} used by {property_name} "
                            f"is not supported by the {type(self).__name__}.",
                            stacklevel=2,
                        )

    def _generate_mock_values(
        self,
        properties: dict[str, dm.MappedProperty],
        config: ViewMockConfig,
        view_id: dm.ViewId,
        default_node_count: int,
        default_nullable_fraction: float,
    ) -> tuple[dict[str, typing.Sequence[ListAbleDataType]], ViewMockData]:
        output: dict[str, typing.Sequence[ListAbleDataType]] = {}
        external = ViewMockData(view_id, self._instance_space)
        values: typing.Sequence[ListAbleDataType]
        for name, prop in properties.items():
            if view_id in _READONLY_PROPERTIES and name in _READONLY_PROPERTIES[view_id]:
                continue

            if name in config.properties:
                generator = config.properties[name]
            elif type(prop.type) in config.property_types:
                generator = config.property_types[type(prop.type)]
            elif isinstance(prop.type, Enum):
                generator = _create_enum_generator(prop.type)
            else:
                warnings.warn(
                    f"Could not generate mock data for property {name} of type {type(prop.type)}", stacklevel=2
                )

                def _only_null_values(count: int) -> list[None]:
                    return [None] * count

                generator = _only_null_values

            config_node_count = config.node_count or default_node_count
            config_null_values = config.null_values or default_nullable_fraction

            null_values = int(prop.nullable and config_node_count * config_null_values)
            node_count = config_node_count - null_values
            if isinstance(prop.type, ListablePropertyType) and prop.type.is_list:
                values = [generator(random.randint(0, 5)) for _ in range(node_count)] + [None] * null_values
            else:
                values = generator(config_node_count - null_values) + [None] * null_values

            if null_values and isinstance(values, list):
                random.shuffle(values)

            output[name] = values
            if isinstance(prop.type, dm.TimeSeriesReference):
                external.timeseries.extend(
                    [
                        TimeSeries(
                            external_id=ts,
                            name=ts,
                            data_set_id=self._data_set_id,
                            is_step=False,
                            is_string=False,
                            metadata={
                                "source": f"Pygen{type(self).__name__}",
                            },
                        )
                        for timeseries_set in values
                        for ts in (
                            cast(list[str], timeseries_set)
                            if isinstance(timeseries_set, list)
                            else [cast(str, timeseries_set)]
                        )
                        if ts
                    ]
                )
            elif isinstance(prop.type, dm.FileReference):
                external.file.extend(
                    [
                        FileMetadata(
                            external_id=file,
                            name=file,
                            source=self._instance_space,
                            data_set_id=self._data_set_id,
                            mime_type="text/plain",
                            metadata={
                                "source": f"Pygen{type(self).__name__}",
                            },
                        )
                        for file_set in values
                        for file in (cast(list[str], file_set) if isinstance(file_set, list) else [cast(str, file_set)])
                        if file
                    ]
                )
            elif isinstance(prop.type, dm.SequenceReference):
                external.sequence.extend(
                    [
                        Sequence(
                            external_id=seq,
                            name=seq,
                            data_set_id=self._data_set_id,
                            columns=[
                                SequenceColumn(
                                    external_id="value",
                                    value_type=cast(Literal["Double"], "DOUBLE"),
                                    metadata={
                                        "source": f"Pygen{type(self).__name__}",
                                    },
                                )
                            ],
                            metadata={
                                "source": f"Pygen{type(self).__name__}",
                            },
                        )
                        for seq_set in values
                        for seq in (cast(list[str], seq_set) if isinstance(seq_set, list) else [cast(str, seq_set)])
                        if seq
                    ]
                )

        return output, external

    @staticmethod
    def _get_other_nodes(
        connection: dm.ViewId,
        outputs: dict[dm.ViewId, ViewMockData],
        leaf_children_by_parent: dict[dm.ViewId, list[dm.ViewId]],
    ) -> list[dm.NodeId]:
        if connection in leaf_children_by_parent:
            sources: list[dm.NodeId] = []
            for child in leaf_children_by_parent[connection]:
                sources.extend(outputs[child].node.as_ids())
        else:
            sources = outputs[connection].node.as_ids()
        return sources

    def _create_edges(
        self, connection: EdgeConnection, this_node: dm.NodeId, sources: list[dm.NodeId], max_edge_count: int
    ) -> list[dm.EdgeApply]:
        end_nodes = random.sample(sources, k=randint(0, max_edge_count))

        edges: list[dm.EdgeApply] = []
        for end_node in end_nodes:
            start_node = this_node
            if connection.direction == "inwards":
                start_node, end_node = end_node, start_node

            edge = dm.EdgeApply(
                space=self._instance_space,
                external_id=f"{start_node.external_id}:{end_node.external_id}",
                type=connection.type,
                start_node=(start_node.space, start_node.external_id),
                end_node=(end_node.space, end_node.external_id),
            )
            edges.append(edge)
        return edges

    def _reset_seed(self, view_id: dm.ViewId) -> None:
        """Resets the seed for the random number generator for the given view.

        The goal is to have reproducible results for each view in a data model.
        """
        if self._seed is None:
            return  # No seed set, nothing to reset
        user_seed: int = self._seed
        view_str = json.dumps(view_id.dump(camel_case=True, include_type=False), sort_keys=True)
        view_seed = user_seed + int(hashlib.md5(view_str.encode("utf-8"), usedforsecurity=False).hexdigest(), 16)
        random.seed(view_seed)
        configs = [self._default_config]
        if view_id in self._view_configs:
            configs.append(self._view_configs[view_id])

        for config in configs:
            if config.reset_seed is not None:
                config.reset_seed(view_seed)
            for generator in config.property_types.values():
                if hasattr(generator, "reset") and isinstance(generator.reset, Callable):  # type: ignore[arg-type]
                    # This is for generators that have a state.
                    generator.reset()

    @staticmethod
    def _set_direct_relation_property(
        this_node: dm.NodeApply, view_id: dm.ViewId, property_name: str, value: dict | list[dict]
    ) -> None:
        if this_node.sources is None:
            this_node.sources = []
        for source in this_node.sources:
            if source.source == view_id:
                if not isinstance(source.properties, dict):
                    source.properties = dict(source.properties) if source.properties else {}
                source.properties[property_name] = value
                break
        else:
            # This is the first property residing in this view
            # for this node
            this_node.sources.append(
                dm.NodeOrEdgeData(
                    source=view_id,
                    properties={property_name: value},
                )
            )

    @staticmethod
    def _to_leaf_children_by_parent(views: list[dm.View]) -> dict[dm.ViewId, list[dm.ViewId]]:
        leaf_children_by_parent: dict[dm.ViewId, set[dm.ViewId]] = defaultdict(set)
        for view in views:
            for parent in view.implements or []:
                leaf_children_by_parent[parent].add(view.as_id())

        leafs: set[dm.ViewId] = set()
        for view_id in TopologicalSorter(leaf_children_by_parent).static_order():
            if view_id not in leaf_children_by_parent:
                leafs.add(view_id)
                continue

            parents = leaf_children_by_parent[view_id] - leafs
            for parent in parents:
                leaf_children_by_parent[view_id].remove(parent)
                leaf_children_by_parent[view_id].update(leaf_children_by_parent[parent])

        return {k: sorted(v, key=lambda x: x.as_tuple()) for k, v in leaf_children_by_parent.items()}


def _create_enum_generator(enum: Enum) -> GeneratorFunction[str]:
    def _enum_generator(count: int) -> list[str]:
        return random.choices(tuple(enum.values.keys()), k=count)

    return _enum_generator


@dataclass
class ViewMockData:
    """Mock data for a given view.

    Args:
        view_id (dm.ViewId): The view id.
        instance_space (str): The instance space.
        is_writeable (bool): Whether the view is writeable. Defaults to True.
        node (dm.NodeApplyList): The nodes.
        edge (dm.EdgeApplyList): The edges.
        timeseries (TimeSeriesList): The timeseries.
        sequence (SequenceList): The sequences.
        file (FileMetadataList): The files.
    """

    view_id: dm.ViewId
    instance_space: str
    is_writeable: bool = True
    node: dm.NodeApplyList = field(default_factory=lambda: dm.NodeApplyList([]))
    edge: dm.EdgeApplyList = field(default_factory=lambda: dm.EdgeApplyList([]))
    timeseries: TimeSeriesList = field(default_factory=lambda: TimeSeriesList([]))
    sequence: SequenceList = field(default_factory=lambda: SequenceList([]))
    file: FileMetadataList = field(default_factory=lambda: FileMetadataList([]))

    @property
    def _node_only(self) -> dm.NodeApplyList:
        nodes = dm.NodeApplyList([])
        for node in self.node:
            # Dumping and loading to avoid mutating the original node
            dumped = node.dump()
            dumped.pop("sources", None)
            nodes.append(dm.NodeApply.load(dumped))
        return nodes

    def dump_yaml(self, folder: Path | str, exclude: set[ResourceType] | None = None) -> None:
        """
        Dumps the mock data to the given folder in yaml format.

        Args:
            folder: The folder to dump the mock data to.
            exclude: The resources to exclude from the dump.
        """
        folder_path = Path(folder)
        if not folder_path.exists():
            folder_path.mkdir(parents=True, exist_ok=True)
        for resource_name in ["node", "edge", "timeseries", "sequence", "file"]:
            if exclude and resource_name in exclude:
                continue
            values = getattr(self, resource_name)
            if values:
                dump_file = folder_path / f"{self.view_id.external_id}.{resource_name}.yaml"
                with dump_file.open("w", encoding="utf-8", newline="\n") as f:
                    f.write(values.dump_yaml())

    def deploy(self, client: CogniteClient, verbose: bool = False) -> None:
        """Deploys the mock data to CDF."""
        with _log_pygen_mock_call(client) as client:
            if client.data_modeling.spaces.retrieve(self.instance_space) is None:
                client.data_modeling.spaces.apply(dm.SpaceApply(self.instance_space, name=self.instance_space))

            if self.node or self.edge:
                if self.is_writeable:
                    nodes = self.node
                else:
                    nodes = self._node_only

                created = client.data_modeling.instances.apply(nodes, self.edge)
                if verbose:
                    print(
                        f"Created {sum(1 for n in created.nodes if n.was_modified)} nodes "
                        f"and {sum(1 for e in created.edges if e.was_modified)} edges"
                    )
            if self.timeseries:
                client.time_series.upsert(self.timeseries)
                if verbose:
                    print(f"Created/Updated {len(self.timeseries)} timeseries")
            if self.sequence:
                client.sequences.upsert(self.sequence)
                if verbose:
                    print(f"Created/Updated {len(self.sequence)} sequences")
            if self.file:
                existing = client.files.retrieve_multiple(
                    external_ids=self.file.as_external_ids(), ignore_unknown_ids=True
                )
                new_files = FileMetadataList([file for file in self.file if file.external_id not in existing])
                for file in new_files:
                    client.files.create(file)
                if verbose:
                    print(f"Created {len(new_files)} files")

    def _repr_html_(self) -> str:
        table = pd.DataFrame(
            [
                {
                    "resource": "node",
                    "count": len(self.node),
                },
                {
                    "resource": "edge",
                    "count": len(self.edge),
                },
                {
                    "resource": "timeseries",
                    "count": len(self.timeseries),
                },
                {
                    "resource": "sequence",
                    "count": len(self.sequence),
                },
                {
                    "resource": "file",
                    "count": len(self.file),
                },
            ]
        )

        return table._repr_html_()  # type: ignore[operator]


_T_ResourceList = typing.TypeVar("_T_ResourceList", bound=typing.Union[TimeSeriesList, SequenceList, FileMetadataList])


class MockData(UserList[ViewMockData]):
    """Mock data for a given data model."""

    @property
    def view_ids(self) -> list[dm.ViewId]:
        return [view_mock_data.view_id for view_mock_data in self]

    @property
    def nodes(self) -> dm.NodeApplyList:
        return dm.NodeApplyList([node for view_mock_data in self for node in view_mock_data.node if node])

    @property
    def writable_nodes(self) -> dm.NodeApplyList:
        return dm.NodeApplyList(
            [
                node
                for view_mock_data in self
                for node in (view_mock_data.node if view_mock_data.is_writeable else view_mock_data._node_only)
                if node
            ]
        )

    @property
    def edges(self) -> dm.EdgeApplyList:
        return dm.EdgeApplyList([edge for view_mock_data in self for edge in view_mock_data.edge if edge])

    @property
    def timeseries(self) -> TimeSeriesList:
        return TimeSeriesList([ts for view_mock_data in self for ts in view_mock_data.timeseries if ts])

    @property
    def sequences(self) -> SequenceList:
        return SequenceList([seq for view_mock_data in self for seq in view_mock_data.sequence if seq])

    @property
    def files(self) -> FileMetadataList:
        return FileMetadataList([file for view_mock_data in self for file in view_mock_data.file if file])

    @property
    def unique_timeseries(self) -> TimeSeriesList:
        return self._unique_resources(self.timeseries)

    @property
    def unique_sequences(self) -> SequenceList:
        return self._unique_resources(self.sequences)

    @property
    def unique_files(self) -> FileMetadataList:
        return self._unique_resources(self.files)

    @staticmethod
    def _unique_resources(resource_list: _T_ResourceList) -> _T_ResourceList:
        seen: set[str] = set()
        unique_resources = type(resource_list)([])
        for resource in resource_list:
            if resource.external_id in seen:
                continue
            if resource.external_id:
                seen.add(resource.external_id)
                unique_resources.append(resource)
        return unique_resources  # type: ignore[return-value]

    def dump_yaml(
        self, folder: Path | str, exclude: set[ResourceType | tuple[str, ResourceType] | str] | None = None
    ) -> None:
        """Dumps the mock data to a folder in yaml format.

        Args:
            folder (Path | str): The folder to dump the mock data to.
            exclude: The resources to exclude from the dump. Can either be a ResourceType,
                a view_external_id or a tuple of view_external_id and ResourceType.
        """
        for view_mock_data in self:
            exclude_view: set[str] | None
            if exclude:
                external_id = view_mock_data.view_id.external_id
                if external_id in exclude:
                    continue
                exclude_view = {
                    item if item in _ResourceTypes else item[1]  # type: ignore[misc]
                    for item in exclude
                    if (isinstance(item, tuple) and item[0] == external_id) or item in _ResourceTypes
                }
            else:
                exclude_view = None
            view_mock_data.dump_yaml(folder, exclude_view)  # type: ignore[arg-type]

    def deploy(
        self,
        client: CogniteClient,
        exclude: set[ResourceType | tuple[str, ResourceType] | str] | None = None,
        verbose: bool = False,
    ) -> None:
        """Deploys the mock data to CDF.

        This means calling the .apply() method for the instances (nodes and edges), and the .upsert() method for
        timeseries and sequences. Files are created using the .create() method.

        Args:
            client (CogniteClient): The client to use for deployment.
            exclude (set[Literal["timeseries", "files", "sequences"]]): The resources to exclude from deployment.
            verbose (bool): Whether to print information about the deployment.
        """
        nodes = self.writable_nodes
        edges = self.edges
        with _log_pygen_mock_call(client) as client:
            if self:
                instance_space = self[0].instance_space
                if client.data_modeling.spaces.retrieve(instance_space) is None:
                    client.data_modeling.spaces.apply(dm.SpaceApply(instance_space, name=instance_space))
                    if verbose:
                        print(f"Created space {instance_space}")

            if nodes or edges:
                # There is an 'edge' if there is an outward and inward edge on two views, we can get duplicated edges.
                # We should remove the duplicates.
                edges = dm.EdgeApplyList({edge.as_id(): edge for edge in edges}.values())

                created = client.data_modeling.instances.apply(
                    nodes,
                    edges,
                    auto_create_start_nodes=True,
                    auto_create_end_nodes=True,
                    auto_create_direct_relations=True,
                )
                if verbose:
                    print(
                        f"Created {sum(1 for n in created.nodes if n.was_modified)} nodes "
                        f"and {sum(1 for e in created.edges if e.was_modified)} edges"
                    )
            if (timeseries := self.unique_timeseries) and (exclude is None or "timeseries" not in exclude):
                client.time_series.upsert(timeseries)
                if verbose:
                    print(f"Created/Updated {len(timeseries)} timeseries")
            if (sequences := self.unique_sequences) and (exclude is None or "sequences" not in exclude):
                client.sequences.upsert(sequences)
                if verbose:
                    print(f"Created/Updated {len(sequences)} sequences")
            if (files := self.unique_files) and (exclude is None or "files" not in exclude):
                existing = set(
                    client.files.retrieve_multiple(
                        external_ids=files.as_external_ids(), ignore_unknown_ids=True
                    ).as_external_ids()
                )
                new_files = FileMetadataList([file for file in files if file.external_id not in existing])
                for file in new_files:
                    client.files.create(file)
                if verbose:
                    print(f"Created {len(new_files)} files")

    def clean(self, client: CogniteClient, delete_space: bool = False, verbose: bool = False) -> None:
        """Cleans the mock data from CDF.

        This means calling the .delete() method for the instances (nodes and edges), timeseries, sequences, and files.

        Args:
            client: The client to use for cleaning.
            delete_space: Whether to delete the instance space.
            verbose: Whether to print information about the cleaning.
        """
        nodes = self.nodes
        edges = self.edges
        with _log_pygen_mock_call(client) as client:
            if nodes or edges:
                client.data_modeling.instances.delete(nodes.as_ids(), edges.as_ids())
                if verbose:
                    print(f"Deleted {len(nodes)} nodes and {len(edges)} edges ")
            if timeseries := self.unique_timeseries:
                client.time_series.delete(external_id=timeseries.as_external_ids(), ignore_unknown_ids=True)
                if verbose:
                    print(f"Deleted {len(timeseries)} timeseries")
            if sequences := self.unique_sequences:
                client.sequences.delete(external_id=sequences.as_external_ids(), ignore_unknown_ids=True)
                if verbose:
                    print(f"Deleted {len(sequences)} sequences")
            if files := self.unique_files:
                try:
                    client.files.delete(external_id=files.as_external_ids())
                except CogniteNotFoundError as e:
                    not_existing = {file["externalId"] for file in e.not_found}
                    files = FileMetadataList([file for file in files if file.external_id not in not_existing])
                    client.files.delete(external_id=files.as_external_ids())
                if verbose:
                    print(f"Deleted {len(files)} files")

            if self and delete_space:
                instance_space = self[0].instance_space
                client.data_modeling.spaces.delete(instance_space)
                if verbose:
                    print(f"Deleted space {instance_space}")

    def _repr_html_(self) -> str:
        table = pd.DataFrame(
            [
                {
                    "resource": "node",
                    "count": len(self.nodes),
                },
                {
                    "resource": "edge",
                    "count": len(self.edges),
                },
                {
                    "resource": "timeseries",
                    "count": len(self.timeseries),
                },
                {
                    "resource": "sequence",
                    "count": len(self.sequences),
                },
                {
                    "resource": "file",
                    "count": len(self.files),
                },
            ]
        )

        return table._repr_html_()  # type: ignore[operator]


T_DataType = typing.TypeVar("T_DataType", bound=DataType)


class GeneratorFunction(typing.Protocol, Generic[T_DataType]):
    """Interface for a function that generates mock data.

    Examples:

        >>> random.seed(42)
        >>> def my_data_generator(count: int) -> list[str]:
        ...     return [
        ...          "".join(random.choices(string.ascii_lowercase + string.ascii_uppercase, k=7))
        ...         for _ in range(count)
        ...     ]
        >>> my_data_generator(5)
        ['HbolMJU', 'evblAbk', 'HClEQaP', 'KriXref', 'SFPLBYt']

    """

    def __call__(self, count: int) -> list[T_DataType]:
        raise NotImplementedError()


class IDGeneratorFunction(typing.Protocol):
    """Interface for a function that generates mock data.

    Examples:

        >>> def my_id_generator(view_id: dm.ViewId, count: int) -> list[str]:
        ...     return [f"{view_id.external_id.casefold()}_{no}" for no in range(count)]
        >>> my_id_generator(dm.ViewId("my_space", "MyView", "v1"), 5)
        ['myview_0', 'myview_1', 'myview_2', 'myview_3', 'myview_4']
    """

    def __call__(self, view_id: dm.ViewId, count: int) -> list[str]:
        raise NotImplementedError()


class _RandomGenerator:
    @staticmethod
    def text(count: int) -> list[str]:
        return ["".join(choices(string.ascii_lowercase + string.ascii_uppercase, k=7)) for _ in range(count)]

    @staticmethod
    def int64(count: int) -> list[int]:
        return [randint(-10_000, 10_000) for _ in range(count)]

    @staticmethod
    def int32(count: int) -> list[int]:
        return [randint(-1000, 1000) for _ in range(count)]

    @staticmethod
    def float64(count: int) -> list[float]:
        return [uniform(-10_000, 10_000) for _ in range(count)]

    @staticmethod
    def float32(count: int) -> list[float]:
        return [round(uniform(-1000, 1000), 2) for _ in range(count)]

    @staticmethod
    def boolean(count: int) -> list[bool]:
        return [choice([True, False]) for _ in range(count)]

    @staticmethod
    def timestamp(count: int) -> list[str]:
        # 1970 - 2050
        return [datetime.fromtimestamp(randint(0, 2556057600)).isoformat(timespec="milliseconds") for _ in range(count)]

    @staticmethod
    def date(count: int) -> list[str]:
        # 1970 - 2050
        return [date.fromtimestamp(randint(0, 2556057600)).isoformat() for _ in range(count)]

    @staticmethod
    def unique_numbers(count: int) -> list[int]:
        """Generates a list of unique numbers."""
        return random.sample(range(0, max(100_000, count)), k=count)

    @staticmethod
    def unique_numbers_multi_calls() -> Callable[[int], list[int]]:
        """Generates a function that generates unique numbers on multiple calls."""
        generated_numbers: set[int] = set()

        def unique_numbers(count: int) -> list[int]:
            nonlocal generated_numbers
            numbers: list[int] = []
            while len(numbers) < count and len(generated_numbers) < 100_000:
                number = randint(0, 100_000)
                if number not in generated_numbers:
                    numbers.append(number)
                    generated_numbers.add(number)
            return numbers

        def reset() -> None:
            nonlocal generated_numbers
            generated_numbers = set()

        unique_numbers.reset = reset  # type: ignore[attr-defined]

        return unique_numbers

    @classmethod
    def timeseries_reference(cls) -> Callable[[int], list[str]]:
        unique_numbers = cls.unique_numbers_multi_calls()

        def timeseries_reference_inner(count: int) -> list[str]:
            return [f"timeseries_{no}" for no in unique_numbers(count)]

        def reset() -> None:
            unique_numbers.reset()  # type: ignore[attr-defined]

        timeseries_reference_inner.reset = reset  # type: ignore[attr-defined]

        return timeseries_reference_inner

    @classmethod
    def file_reference(cls) -> Callable[[int], list[str]]:
        unique_numbers = cls.unique_numbers_multi_calls()

        def file_reference_inner(count: int) -> list[str]:
            return [f"file_{no}" for no in unique_numbers(count)]

        def reset() -> None:
            unique_numbers.reset()  # type: ignore[attr-defined]

        file_reference_inner.reset = reset  # type: ignore[attr-defined]

        return file_reference_inner

    @classmethod
    def sequence_reference(cls) -> Callable[[int], list[str]]:
        unique_numbers = cls.unique_numbers_multi_calls()

        def sequence_reference_inner(count: int) -> list[str]:
            return [f"sequence_{no}" for no in unique_numbers(count)]

        def reset() -> None:
            unique_numbers.reset()  # type: ignore[attr-defined]

        sequence_reference_inner.reset = reset  # type: ignore[attr-defined]

        return sequence_reference_inner

    @classmethod
    def json(cls, count: int) -> list[dict]:
        return [
            {
                key: value
                for key, value in zip(
                    cls.text(3),
                    [cls.text(1)[0], cls.int32(1)[0], cls.float32(1)[0]],
                    strict=False,
                )
            }
            for _ in range(count)
        ]

    @classmethod
    def node_id(cls, view_id: dm.ViewId, count: int) -> list[str]:
        return [f"{view_id.external_id.casefold()}_{no}" for no in cls.unique_numbers(count)]


def _create_default_property_types() -> dict[type[dm.PropertyType], GeneratorFunction]:
    return {
        dm.Text: _RandomGenerator.text,
        dm.Int64: _RandomGenerator.int32,
        dm.Int32: _RandomGenerator.int64,
        dm.Float64: _RandomGenerator.float64,
        dm.Float32: _RandomGenerator.float32,
        dm.Boolean: _RandomGenerator.boolean,
        dm.Timestamp: _RandomGenerator.timestamp,
        dm.Date: _RandomGenerator.date,
        dm.TimeSeriesReference: cast(GeneratorFunction, _RandomGenerator.timeseries_reference()),
        dm.FileReference: cast(GeneratorFunction, _RandomGenerator.file_reference()),
        dm.SequenceReference: cast(GeneratorFunction, _RandomGenerator.sequence_reference()),
        dm.Json: _RandomGenerator.json,
    }


@dataclass
class ViewMockConfig:
    """This class contains parameters for configuration of how the mock
    data should be generated for a given view.

    This controls how many nodes and edges should be generated, and how to generate mock data for the different
    property types and relations (direct relations + edges).

    The 'properties' and 'property_types' parameters can be used to override the default mock data generation for
    specific properties and property types. The 'properties' parameter takes precedence over the 'property_types'

    Note that this gives a very granular control over how to generate mock data, but it can be cumbersome to use.
    For most use cases, it is recommended to use the 'default_config' parameter of the MockGenerator class instead.

    Args:
        node_count: The number of nodes to generate for this view.
        max_edge_per_type: The maximum number of edges to generate per edge type for this view.
        null_values: The fraction of nullable properties that should be null for this view.
        node_id_generator: How to generate node ids.
        property_types: How to generate mock
            data for the different property types. The keys are the property types, and the values are functions
            that take the number of nodes as input and return a list of property values.
        properties: How to generate mock data for the different
            properties. The keys are the property names, and the values are functions that take the number of nodes
            as input and return a list of property values.
        reset_seed: Optional function for resetting seed for random generator.

    """

    node_count: int | None = None
    max_edge_per_type: int | None = None
    null_values: float | None = None
    node_id_generator: IDGeneratorFunction = _RandomGenerator.node_id  # type: ignore[assignment]
    property_types: dict[type[dm.PropertyType], GeneratorFunction] = field(
        default_factory=_create_default_property_types
    )
    properties: dict[str, GeneratorFunction] = field(default_factory=dict)
    reset_seed: Callable[[int], None] | None = None

    def __post_init__(self):
        if self.null_values is not None and (self.null_values < 0 or self.null_values > 1):
            raise ValueError("null_values must be between 0 and 1")
        if self.node_count is not None and self.node_count <= 0:
            raise ValueError("node_count must be greater than 0")
        if self.max_edge_per_type is not None and self.max_edge_per_type < 0:
            raise ValueError("max_edge_per_type must be greater than 0")
        for k, v in _create_default_property_types().items():
            if k not in self.property_types:
                self.property_types[k] = v
            elif self.property_types[k] is not v:
                expected = self.property_types[k](3)
                if len(expected) != 3:
                    raise ValueError(
                        f"Invalid Custom Random Generator property_types[{k}](3) must return a list of length 3"
                    )
                expected = self.property_types[k](5)
                if len(expected) != 5:
                    raise ValueError(
                        f"Invalid Custom Random Generator property_types[{k}](5) must return a list of length 5"
                    )


def _create_faker_config(seed: int | None = None) -> ViewMockConfig:
    generator = FakerGenerators(seed)
    return ViewMockConfig(
        node_id_generator=generator.id_generator,  # type: ignore[arg-type]
        property_types={
            dm.Text: generator.text,
            dm.Int64: generator.int64,
            dm.Int32: generator.int32,
            dm.Float64: generator.float64,
            dm.Float32: generator.float32,
            dm.Boolean: generator.boolean,
            dm.Json: generator.json,
            dm.Timestamp: generator.timestamp,
            dm.Date: generator.date,
            dm.FileReference: generator.file,
            dm.SequenceReference: generator.sequence,
            dm.TimeSeriesReference: generator.timeseries,
        },
        reset_seed=generator.reset,
    )


class FakerGenerators:
    def __init__(self, seed: int | None = None):
        try:
            from faker import Faker
        except ImportError as e:
            raise PygenImportError("Faker is required for this feature. Install it with 'pip install faker'") from e

        if seed is not None:
            self.reset(seed)
        self.faker: Faker = Faker()

    def reset(self, seed: int) -> None:
        try:
            from faker import Faker
        except ImportError as e:
            raise PygenImportError("Faker is required for this feature. Install it with 'pip install faker'") from e
        Faker.seed(seed)
        self.faker = Faker()

    def id_generator(self, view_id: dm.ViewId, node_count: int) -> list[str]:
        return [f"{view_id.external_id}:{self.faker.unique.first_name()}" for _ in range(node_count)]

    def text(self, count: int) -> list[str]:
        return [self.faker.sentence() for _ in range(count)]

    def int64(self, count: int) -> list[int]:
        info = np.iinfo(np.int64)
        return [self.faker.random.randint(int(info.min) + 1, int(info.max) - 1) for _ in range(count)]

    def int32(self, count: int) -> list[int]:
        info = np.iinfo(np.int32)
        return [self.faker.random.randint(int(info.min) + 1, int(info.max) - 1) for _ in range(count)]

    def float64(self, count: int) -> list[float]:
        info = np.finfo(np.float64)
        return [
            round(self.faker.random.uniform(float(info.min) / 2, float(info.max) / 2), info.precision)
            for _ in range(count)
        ]

    def float32(self, count: int) -> list[float]:
        return [round(float(np.float32(self.faker.random.uniform(-1000, 1000))), 2) for _ in range(count)]

    def boolean(self, count: int) -> list[bool]:
        return [self.faker.pybool() for _ in range(count)]

    def json(self, count: int) -> list[dict]:
        return [
            {self.text(1)[0]: self.text(1)[0] for _ in range(self.faker.random.randint(0, 5))} for _ in range(count)
        ]

    def timestamp(self, count: int) -> list[datetime]:
        return [self.faker.date_time_this_year() for _ in range(count)]

    def date(self, count: int) -> list[date]:
        return [cast(date, self.faker.date_this_year()) for _ in range(count)]

    def file(self, count: int) -> list[str]:
        return [f"file_{self.faker.unique.first_name().casefold()}" for _ in range(count)]

    def sequence(self, count: int) -> list[str]:
        return [f"sequence_{self.faker.unique.first_name().casefold()}" for _ in range(count)]

    def timeseries(self, count: int) -> list[str]:
        return [f"timeseries_{self.faker.unique.first_name().casefold()}" for _ in range(count)]


def _connected_views(views: typing.Sequence[dm.View]) -> Iterable[list[dm.View]]:
    """Find the connected views in the data model."""
    view_by_id = {view.as_id(): view for view in views}

    graph: dict[dm.ViewId, set[dm.ViewId]] = defaultdict(set)
    for view in views:
        dependencies = set()
        for prop in view.properties.values():
            source: dm.ViewId | None = None
            if isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation) and prop.source:
                source = prop.source
            elif isinstance(prop, MultiEdgeConnection):
                source = prop.source

            if source and source in view_by_id:
                dependencies.add(source)
            elif source:
                warnings.warn(
                    f"The view {source} referenced by {view.as_id()} is not in the data model. Skipping it.",
                    stacklevel=2,
                )

        graph[view.as_id()] |= dependencies
        for dep in dependencies:
            graph[dep] |= {view.as_id()}

    for components in _connected_components(graph):
        yield [view_by_id[view_id] for view_id in components]


_T_Component = typing.TypeVar("_T_Component")


def _connected_components(graph: dict[_T_Component, set[_T_Component]]) -> list[list[_T_Component]]:
    """Finds the connected components in a graph."""
    seen = set()

    def component(search_node: _T_Component) -> Iterable[_T_Component]:
        neighbors = {search_node}
        while neighbors:
            search_node = neighbors.pop()
            seen.add(search_node)
            neighbors |= graph[search_node] - seen
            yield search_node

    components = []
    for node in graph:
        if node not in seen:
            components.append(list(component(node)))

    return components


@contextmanager
def _log_pygen_mock_call(client: CogniteClient) -> typing.Generator[CogniteClient, None, None]:
    """Context manager for logging Pygen usage."""
    current_client_name = client.config.client_name
    # The client name is used for aggregated logging of Pygen Usage
    client.config.client_name = f"CognitePygen:{__version__}:MockGenerator"
    yield client
    client.config.client_name = current_client_name
