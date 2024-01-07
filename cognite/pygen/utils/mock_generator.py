"""This module contains the mock generator for the pygen package. It can be used to generate mock nodes,
edges, timeseries, sequences, and files for a given data model/views.
"""
from __future__ import annotations

import random
import string
import typing
from collections import UserList, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from random import choice, choices, randint, uniform
from typing import Generic, Literal, cast

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
from cognite.client.data_classes.data_modeling.data_types import ListablePropertyType
from cognite.client.data_classes.data_modeling.views import MultiEdgeConnection
from typing_extensions import TypeAlias

DataType: TypeAlias = typing.Union[int, float, bool, str, dict, None]
ListAbleDataType: TypeAlias = typing.Union[
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


class MockGenerator:
    """Mock generator for the pygen package. It can be used to generate mock nodes, edges, timeseries,
    sequences, and files for a given data model/views.

    Args:
        views (List[View]): The views to generate mock data for.
        view_configs (dict[ViewId, ViewMockConfig]): Configuration for how to generate mock data for the different
            views. The keys are the view ids, and the values are the configuration for the view.
        default_config (ViewMockConfig): Default configuration for how to generate mock data for the different
            views.
        data_set_id (int): The data set id to use for TimeSeries, Sequnces, and FileMetadata.
        seed (int): The seed to use for the random number generator.
    """

    def __init__(
        self,
        views: typing.Sequence[dm.View],
        instance_space: str = "sandbox",
        view_configs: dict[dm.ViewId, ViewMockConfig] | None = None,
        default_config: ViewMockConfig | None = None,
        data_set_id: int | None = None,
        seed: int | None = None,
    ):
        self.views = dm.ViewList(views)
        self._instance_space = instance_space
        self.view_configs = view_configs or {}
        self.default_config = default_config or ViewMockConfig()
        self.data_set_id = data_set_id
        self.seed = seed

    @classmethod
    def from_data_model(cls, data_model_id: DataModelIdentifier, client: CogniteClient) -> MockGenerator:
        data_model = client.data_modeling.data_models.retrieve(  # type: ignore[call-overload]
            id=data_model_id,
            inline_views=True,
        ).latest_version()

        return cls(
            views=data_model.views,
        )

    def generate_mock_data(self) -> MockData:
        """Generates mock data for the given data model/views.

        Returns:
            MockData: The generated mock data.
        """
        if self.seed:
            random.seed(self.seed)
        mock_data = MockData()
        for components in _connected_views(self.views):
            data = self._generate_views_mock_data(components)
            mock_data.extend(data)
        return mock_data

    def _generate_views_mock_data(self, views: list[dm.View]) -> MockData:
        outputs = self._generate_mock_nodes(views)
        self._generate_mock_relations(views, outputs)
        return MockData(outputs.values())

    def _generate_mock_nodes(self, views: list[dm.View]) -> dict[dm.ViewId, ViewMockData]:
        output: dict[dm.ViewId, ViewMockData] = {}
        for view in views:
            mapped_properties = {
                name: prop
                for name, prop in view.properties.items()
                if isinstance(prop, dm.MappedProperty) and not isinstance(prop.type, dm.DirectRelation)
            }

            node_type: dm.DirectRelationReference | None = None
            if isinstance(view.filter, dm.filters.Equals):
                node_type = dm.DirectRelationReference.load(view.filter.dump()["equals"]["value"])

            view_id = view.as_id()
            config = self.view_configs.get(view_id, self.default_config)
            properties, external = self._generate_mock_values(mapped_properties, config, view.as_id())
            node_ids = config.node_id_generator(view_id, config.node_count)

            nodes = [
                dm.NodeApply(
                    space=self._instance_space,
                    external_id=node_id,
                    type=node_type,
                    sources=[
                        dm.NodeOrEdgeData(
                            source=view.as_id(),
                            properties=dict(zip(properties.keys(), props)),
                        )
                    ]
                    if props
                    else [],
                )
                for node_id, *props in zip(node_ids, *properties.values())
            ]
            output[view.as_id()] = ViewMockData(
                view.as_id(),
                node=dm.NodeApplyList(nodes),
                timeseries=TimeSeriesList(external.timeseries),
                sequence=SequenceList(external.sequence),
                file=FileMetadataList(external.file),
            )
        return output

    def _generate_mock_relations(self, views: list[dm.View], outputs: dict[dm.ViewId, ViewMockData]) -> None:
        for view in views:
            connection_properties = {
                name: prop
                for name, prop in view.properties.items()
                if (isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation))
                or isinstance(prop, dm.ConnectionDefinition)
            }
            if not connection_properties:
                continue
            view_id = view.as_id()
            config = self.view_configs.get(view_id, self.default_config)
            for node in outputs[view_id].node:
                for name, connection in connection_properties.items():
                    if isinstance(connection, MultiEdgeConnection):
                        sources = outputs[connection.source].node.as_ids()
                        if config.allow_edge_reuse or len(sources) < config.max_edge_count:
                            end_nodes = choices(sources, k=randint(0, config.max_edge_count))
                        else:
                            end_nodes = random.sample(sources, k=randint(0, config.max_edge_count))
                        for end_node in end_nodes:
                            start_node = node.as_id()
                            if connection.direction == "inwards":
                                start_node, end_node = end_node, start_node

                            edge = dm.EdgeApply(
                                space=self._instance_space,
                                external_id=f"{start_node.external_id}:{end_node.external_id}",
                                type=connection.type,
                                start_node=(start_node.space, start_node.external_id),
                                end_node=(end_node.space, end_node.external_id),
                            )
                            outputs[view_id].edge.append(edge)
                    elif (
                        isinstance(connection, dm.MappedProperty)
                        and isinstance(connection.type, dm.DirectRelation)
                        and connection.source
                    ):
                        if (not connection.nullable or random.random() < (1 - config.null_values)) and (
                            sources := outputs[connection.source].node.as_ids()
                        ):
                            other_node = choice(sources)
                            if node.sources is None:
                                node.sources = []
                            for source in node.sources:
                                if source.source == view_id:
                                    if not isinstance(source.properties, dict):
                                        source.properties = dict(source.properties) if source.properties else {}
                                    source.properties[name] = {
                                        "space": other_node.space,
                                        "externalId": other_node.external_id,
                                    }
                                    break
                            else:
                                node.sources.append(
                                    dm.NodeOrEdgeData(
                                        source=view_id,
                                        properties={
                                            name: {"space": other_node.space, "externalId": other_node.external_id}
                                        },
                                    )
                                )
                    else:
                        raise NotImplementedError(f"Connection {type(connection)} not implemented")

    def _generate_mock_values(
        self, properties: dict[str, dm.MappedProperty], config: ViewMockConfig, view_id: dm.ViewId
    ) -> tuple[dict[str, typing.Sequence[ListAbleDataType]], ViewMockData]:
        output: dict[str, typing.Sequence[ListAbleDataType]] = {}
        external = ViewMockData(view_id)
        values: typing.Sequence[ListAbleDataType]
        for name, prop in properties.items():
            if name in config.properties:
                generator = config.properties[name]
            elif type(prop.type) in config.property_types:
                generator = config.property_types[type(prop.type)]
            elif type(prop.type) in DEFAULT_PROPERTY_TYPES:
                generator = DEFAULT_PROPERTY_TYPES[type(prop.type)]
            else:
                raise ValueError(f"Could not generate mock data for property {name} of type {type(prop.type)}")

            null_values = int(prop.nullable and config.node_count * config.null_values)
            node_count = config.node_count - null_values
            if isinstance(prop.type, ListablePropertyType) and prop.type.is_list:
                values = [generator(random.randint(0, 5)) for _ in range(node_count)] + [None] * null_values
            else:
                values = generator(config.node_count - null_values) + [None] * null_values
            if null_values and isinstance(values, list):
                random.shuffle(values)

            output[name] = values
            if isinstance(prop.type, dm.TimeSeriesReference):
                external.timeseries.extend(
                    [
                        TimeSeries(
                            external_id=ts,
                            name=ts,
                            data_set_id=self.data_set_id,
                            is_step=False,
                            is_string=False,
                        )
                        for timeseries_set in values
                        for ts in (
                            cast(list[str], timeseries_set)
                            if isinstance(timeseries_set, list)
                            else [cast(str, timeseries_set)]
                        )
                    ]
                )
            elif isinstance(prop.type, dm.FileReference):
                external.file.extend(
                    [
                        FileMetadata(
                            external_id=file,
                            name=file,
                            source=self._instance_space,
                            data_set_id=self.data_set_id,
                            mime_type="text/plain",
                        )
                        for file_set in values
                        for file in (cast(list[str], file_set) if isinstance(file_set, list) else [cast(str, file_set)])
                    ]
                )
            elif isinstance(prop.type, dm.SequenceReference):
                external.sequence.extend(
                    [
                        Sequence(
                            external_id=seq,
                            name=seq,
                            data_set_id=self.data_set_id,
                            columns=[SequenceColumn(external_id="value", value_type=cast(Literal["Double"], "DOUBLE"))],
                        )
                        for seq_set in values
                        for seq in (cast(list[str], seq_set) if isinstance(seq_set, list) else [cast(str, seq_set)])
                    ]
                )

        return output, external


@dataclass
class ViewMockData:
    """Mock data for a given view."""

    view_id: dm.ViewId
    node: dm.NodeApplyList = field(default_factory=lambda: dm.NodeApplyList([]))
    edge: dm.EdgeApplyList = field(default_factory=lambda: dm.EdgeApplyList([]))
    timeseries: TimeSeriesList = field(default_factory=lambda: TimeSeriesList([]))
    sequence: SequenceList = field(default_factory=lambda: SequenceList([]))
    file: FileMetadataList = field(default_factory=lambda: FileMetadataList([]))

    def dump_yaml(self, folder: Path) -> None:
        for resource_name in ["node", "edge", "timeseries", "sequence", "file"]:
            values = getattr(self, resource_name)
            if values:
                (folder / f"{self.view_id.external_id}.{resource_name}.yaml").write_text(values.dump_yaml())

    def deploy(self, client: CogniteClient) -> None:
        if self.node or self.edge:
            client.data_modeling.instances.apply(self.node, self.edge)
        if self.timeseries:
            client.time_series.upsert(self.timeseries)
        if self.sequence:
            client.sequences.upsert(self.sequence)
        if self.file:
            existing = client.files.retrieve_multiple(external_ids=self.file.as_external_ids(), ignore_unknown_ids=True)
            new_files = FileMetadataList([file for file in self.file if file.external_id not in existing])
            for file in new_files:
                client.files.create(file)


class MockData(UserList[ViewMockData]):
    """Mock data for a given data model."""

    def dump_yaml(self, folder: Path) -> None:
        """Dumps the mock data to a folder.

        Args:
            folder (Path): The folder to dump the mock data to.
        """
        for view_mock_data in self:
            view_mock_data.dump_yaml(folder)

    def deploy(self, client: CogniteClient) -> None:
        """Deploys the mock data to CDF.

        Args:
            client (CogniteClient): The client to use for deployment.
        """
        nodes = dm.NodeApplyList([node for view_mock_data in self for node in view_mock_data.node if node])
        edges = dm.EdgeApplyList([edge for view_mock_data in self for edge in view_mock_data.edge if edge])
        if nodes or edges:
            client.data_modeling.instances.apply(nodes, edges)
        timeseries = TimeSeriesList([ts for view_mock_data in self for ts in view_mock_data.timeseries if ts])
        if timeseries:
            client.time_series.upsert(timeseries)
        sequences = SequenceList([seq for view_mock_data in self for seq in view_mock_data.sequence if seq])
        if sequences:
            client.sequences.upsert(sequences)
        files = FileMetadataList([file for view_mock_data in self for file in view_mock_data.file if file])
        if files:
            existing = client.files.retrieve_multiple(external_ids=files.as_external_ids(), ignore_unknown_ids=True)
            new_files = FileMetadataList([file for file in files if file.external_id not in existing])
            for file in new_files:
                client.files.create(file)


T_DataType = typing.TypeVar("T_DataType", bound=DataType)


class GeneratorFunction(typing.Protocol, Generic[T_DataType]):
    """Interface for a function that generates mock data."""

    def __call__(self, count: int) -> list[T_DataType]:
        raise NotImplementedError()


class IDGeneratorFunction(typing.Protocol):
    """Interface for a function that generates mock data."""

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
        return [uniform(-1000, 1000) for _ in range(count)]

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
        return random.sample(range(0, max(1000, count)), k=count)

    @classmethod
    def timeseries_reference(cls, count: int) -> list[str]:
        return [f"timeseries_{no}" for no in cls.unique_numbers(count)]

    @classmethod
    def file_reference(cls, count: int) -> list[str]:
        return [f"file_{no}" for no in cls.unique_numbers(count)]

    @classmethod
    def sequence_reference(cls, count: int) -> list[str]:
        return [f"sequence_{no}" for no in cls.unique_numbers(count)]

    @classmethod
    def json(cls, count: int) -> list[dict]:
        return [
            {
                key: value
                for key, value in zip(
                    cls.text(3),
                    [cls.text(1)[0], cls.int32(1)[0], cls.float32(1)[0]],
                )
            }
            for _ in range(count)
        ]

    @classmethod
    def node_id(cls, view_id: dm.ViewId, count: int) -> list[str]:
        return [f"{view_id.external_id.casefold()}_{no}" for no in cls.unique_numbers(count)]


DEFAULT_PROPERTY_TYPES: dict[type[dm.PropertyType], GeneratorFunction] = {
    dm.Text: cast(GeneratorFunction, _RandomGenerator.text),
    dm.Int64: cast(GeneratorFunction, _RandomGenerator.int32),
    dm.Int32: cast(GeneratorFunction, _RandomGenerator.int64),
    dm.Float64: cast(GeneratorFunction, _RandomGenerator.float64),
    dm.Float32: cast(GeneratorFunction, _RandomGenerator.float32),
    dm.Boolean: cast(GeneratorFunction, _RandomGenerator.boolean),
    dm.Timestamp: cast(GeneratorFunction, _RandomGenerator.timestamp),
    dm.Date: cast(GeneratorFunction, _RandomGenerator.date),
    dm.TimeSeriesReference: cast(GeneratorFunction, _RandomGenerator.timeseries_reference),
    dm.FileReference: cast(GeneratorFunction, _RandomGenerator.file_reference),
    dm.SequenceReference: cast(GeneratorFunction, _RandomGenerator.sequence_reference),
    dm.Json: cast(GeneratorFunction, _RandomGenerator.json),
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
        node_count (int): The number of nodes to generate.
        max_edge_count (int): The number of edges to generate.
        allow_edge_reuse (bool): Whether to allow edges to be reused.
        null_values (float): The fraction of nullable properties that should be null.
        node_id_generator (IDGeneratorFunction): How to generate node ids.
        property_types (dict[type[dm.PropertyType], GeneratorFunction]): How to generate mock
            data for the different property types. The keys are the property types, and the values are functions
            that take the number of nodes as input and return a list of property values.
        properties (dict[str, GeneratorFunction]): How to generate mock data for the different
            properties. The keys are the property names, and the values are functions that take the number of nodes
            as input and return a list of property values.

    """

    node_count: int = 5
    max_edge_count: int = 3
    allow_edge_reuse: bool = False
    null_values: float = 0.25
    node_id_generator: IDGeneratorFunction = _RandomGenerator.node_id
    property_types: dict[type[dm.PropertyType], GeneratorFunction] = field(
        default_factory=lambda: dict(DEFAULT_PROPERTY_TYPES)
    )
    properties: dict[str, GeneratorFunction] = field(default_factory=lambda: {})

    def __post_init__(self):
        if self.null_values < 0 or self.null_values > 1:
            raise ValueError("null_values must be between 0 and 1")


def _connected_views(views: typing.Sequence[dm.View]) -> Iterable[list[dm.View]]:
    """Find the connected views in the data model."""
    graph: dict[dm.ViewId, set[dm.ViewId]] = defaultdict(set)
    for view in views:
        dependencies = set()
        for prop in view.properties.values():
            if isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation) and prop.source:
                dependencies.add(prop.source)
            elif isinstance(prop, MultiEdgeConnection):
                dependencies.add(prop.source)
        graph[view.as_id()] |= dependencies
        for dep in dependencies:
            graph[dep] |= {view.as_id()}

    view_by_id = {view.as_id(): view for view in views}
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
