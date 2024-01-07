from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import TypeVar, Union

import numpy as np
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
from cognite.client.data_classes.data_modeling.data_types import ListablePropertyType
from cognite.client.data_classes.data_modeling.instances import PropertyValue
from cognite.client.data_classes.data_modeling.views import MultiEdgeConnection
from faker import Faker

from tests.constants import OMNI_SDK

MODEL_DIR = Path(__file__).resolve().parent

MODEL_FILE = MODEL_DIR / "model.yaml"
DATA_DIR = MODEL_DIR / "data"


def main():
    Faker.seed(42)
    faker = Faker()
    data_model = dm.DataModel[dm.View].load(MODEL_FILE.read_text())
    interfaces = {parent for view in data_model.views for parent in view.implements or []}
    views = [
        view
        for view in data_model._views
        # The empty view is used for testing and should not have mock data, neither should interfaces
        if view.external_id != "Empty" and view.as_id() not in interfaces
    ]
    # Not writeable views need to be included as they might be used in connections (edges)
    {view.as_id() for view in data_model._views if not view.writable}

    for component in connected_views(views):
        mock_data = generate_mock_data(component, node_count=5, edge_count=3, faker=faker)

        for data in mock_data:
            for field_ in fields(data):
                if field_.name == "view_id":
                    continue
                if data.view_id.external_id == "Implementation1NonWriteable" and field_.name == "node":
                    continue
                resources = getattr(data, field_.name)
                if not resources:
                    continue
                (DATA_DIR / f"{data.view_id.external_id}.{field_.name}.yaml").write_text(resources.dump_yaml())
            print(f"Generated {len(data.node)} nodes and {len(data.edge)} edges for {data.view_id.external_id}")


def connected_views(views: list[dm.View]) -> Iterable[list[dm.View]]:
    """
    Find the connected views in the data model.
    """
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
    for components in connected_components(graph):
        yield [view_by_id[view_id] for view_id in components]


T_Component = TypeVar("T_Component")


def connected_components(graph: dict[T_Component, set[T_Component]]) -> list[list[T_Component]]:
    """
    Find the connected components in a graph.
    """
    seen = set()

    def component(search_node: T_Component) -> Iterable[T_Component]:
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


@dataclass
class Data:
    view_id: dm.ViewId
    node: dm.NodeApplyList = field(default_factory=lambda: dm.NodeApplyList([]))
    edge: dm.EdgeApplyList = field(default_factory=lambda: dm.EdgeApplyList([]))
    timeseries: TimeSeriesList = field(default_factory=lambda: TimeSeriesList([]))
    sequence: SequenceList = field(default_factory=lambda: SequenceList([]))
    file: FileMetadataList = field(default_factory=lambda: FileMetadataList([]))


def generate_mock_data(views: list[dm.View], node_count: int, edge_count: int, faker: Faker) -> list[Data]:
    outputs: dict[dm.ViewId, Data] = {}
    for view in views:
        view_id = view.as_id()
        outputs[view_id] = output = Data(view_id=view.as_id())
        mapped_properties = {
            k: v
            for k, v in view.properties.items()
            if isinstance(v, dm.MappedProperty) and not isinstance(v.type, dm.DirectRelation)
        }

        node_type: Union[dm.DirectRelationReference, None] = None
        if isinstance(view.filter, dm.filters.Equals):
            node_type = dm.DirectRelationReference.load(view.filter.dump()["equals"]["value"])
        for _ in range(node_count):
            if mapped_properties:
                properties, external = generate_mock_values(mapped_properties, faker, view_id)
                output.file.extend(external.file)
                output.timeseries.extend(external.timeseries)
                output.sequence.extend(external.sequence)
                sources = [
                    dm.NodeOrEdgeData(
                        source=view.as_id(),
                        properties=properties,
                    )
                ]
            else:
                sources = []

            node = dm.NodeApply(
                space=OMNI_SDK.instance_space,
                external_id=f"{view.external_id}:{faker.unique.first_name()}",
                sources=sources,
                type=node_type,
            )

            output.node.append(node)
    for view in views:
        view_id = view.as_id()
        connection_properties = {
            k: v
            for k, v in view.properties.items()
            if (isinstance(v, dm.MappedProperty) and isinstance(v.type, dm.DirectRelation))
            or isinstance(v, dm.ConnectionDefinition)
        }
        if not connection_properties:
            continue
        for node in outputs[view_id].node:
            for name, connection in connection_properties.items():
                if isinstance(connection, MultiEdgeConnection):
                    sources = outputs[connection.source].node.as_ids()
                    for _ in range(faker.random.randint(0, edge_count)):
                        start_node = node.as_id()
                        end_node = faker.random.choice(sources)
                        if connection.direction == "inwards":
                            start_node, end_node = end_node, start_node

                        edge = dm.EdgeApply(
                            space=OMNI_SDK.instance_space,
                            external_id=f"{view.external_id}:{faker.unique.first_name()}",
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
                    if faker.random.random() < 0.25 and (sources := outputs[connection.source].node.as_ids()):
                        other_node = faker.random.choice(sources)
                        for source in node.sources:
                            if source.source == view_id:
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

    return list(outputs.values())


def generate_mock_values(
    properties: dict[str, dm.MappedProperty], faker: Faker, view_id: dm.ViewId
) -> tuple[dict[str, PropertyValue], Data]:
    output = {}
    external = Data(view_id)
    for name, mapped in properties.items():
        if mapped.nullable and faker.random.random() < 0.5:
            output[name] = None
            continue

        if isinstance(mapped.type, ListablePropertyType) and mapped.type.is_list:
            output[name] = [create_value(mapped.type, faker) for _ in range(faker.random.randint(0, 5))]
        else:
            output[name] = create_value(mapped.type, faker)

        if isinstance(mapped.type, dm.CDFExternalIdReference):
            references = output[name] if isinstance(output[name], list) else [output[name]]
            for ref in references:
                if isinstance(mapped.type, dm.TimeSeriesReference):
                    external.timeseries.append(
                        TimeSeries(
                            external_id=ref,
                            name=ref,
                            # These must be set to ensure comparison work in the
                            # 'deploy' command.
                            is_step=False,
                            is_string=False,
                        )
                    )
                elif isinstance(mapped.type, dm.SequenceReference):
                    external.sequence.append(
                        Sequence(
                            external_id=ref,
                            name=ref,
                            columns=[SequenceColumn(external_id="value", value_type="DOUBLE")],
                        )
                    )
                elif isinstance(mapped.type, dm.FileReference):
                    external.file.append(
                        FileMetadata(
                            external_id=ref,
                            name=ref,
                            source=OMNI_SDK.instance_space,
                            mime_type="text/plain",
                        )
                    )
                else:
                    raise NotImplementedError(mapped.type)

    return output, external


def create_value(prop: dm.PropertyType, faker) -> PropertyValue:
    if isinstance(prop, dm.Text):
        return faker.sentence()
    elif isinstance(prop, dm.Int64):
        info = np.iinfo(np.int64)
        return faker.random.randint(int(info.min) + 1, int(info.max) - 1)
    elif isinstance(prop, dm.Int32):
        info = np.iinfo(np.int32)
        return faker.random.randint(int(info.min) + 1, int(info.max) - 1)
    elif isinstance(prop, dm.Float64):
        info = np.finfo(np.float64)
        return round(faker.random.uniform(float(info.min) / 2, float(info.max) / 2), info.precision)
    elif isinstance(prop, dm.Float32):
        number = np.float32(faker.random.uniform(-1000, 1000))
        return round(float(number), 2)
    elif isinstance(prop, dm.Boolean):
        return faker.pybool()
    elif isinstance(prop, dm.Json):
        return {
            create_value(dm.Text(), faker): create_value(dm.Text(), faker) for _ in range(faker.random.randint(0, 5))
        }
    elif isinstance(prop, dm.Timestamp):
        return faker.date_time_this_year()
    elif isinstance(prop, dm.Date):
        return faker.date_this_year()
    elif isinstance(prop, dm.CDFExternalIdReference):
        return f"{prop._type}_{faker.unique.first_name().casefold()}"
    else:
        raise NotImplementedError(prop)


if __name__ == "__main__":
    main()
