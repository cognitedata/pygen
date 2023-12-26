import random
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Union

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
from faker import Faker

from tests.constants import OMNI_SDK

MODEL_DIR = Path(__file__).resolve().parent

MODEL_FILE = MODEL_DIR / "model.yaml"
DATA_DIR = MODEL_DIR / "data"


def main():
    Faker.seed(42)
    data_model = dm.DataModel[dm.View].load(MODEL_FILE.read_text())
    interfaces = {parent for view in data_model.views for parent in view.implements or []}

    for view in data_model.views:
        view: dm.View
        if not view.writable or view.as_id() in interfaces:
            continue

        mock_data = generate_mock_data(view, node_count=5)
        for field_ in fields(mock_data):
            resources = getattr(mock_data, field_.name)
            if not resources:
                continue
            (DATA_DIR / f"{view.external_id}.{field_.name}.yaml").write_text(resources.dump_yaml())
        print(f"Generated {len(mock_data.node)} nodes for {view.external_id}")


@dataclass
class Data:
    node: dm.NodeApplyList = field(default_factory=lambda: dm.NodeApplyList([]))
    edge: dm.EdgeApplyList = field(default_factory=lambda: dm.EdgeApplyList([]))
    timeseries: TimeSeriesList = field(default_factory=lambda: TimeSeriesList([]))
    sequence: SequenceList = field(default_factory=lambda: SequenceList([]))
    file: FileMetadataList = field(default_factory=lambda: FileMetadataList([]))


def generate_mock_data(view: dm.View, node_count: int) -> Data:
    faker = Faker()
    output = Data()
    node_type: Union[dm.DirectRelationReference, None] = None
    if isinstance(view.filter, dm.filters.Equals):
        node_type = dm.DirectRelationReference.load(view.filter.dump()["equals"]["value"])
    for _ in range(node_count):
        mapped_properties = {k: v for k, v in view.properties.items() if isinstance(v, dm.MappedProperty)}
        properties, external = generate_mock_values(mapped_properties, faker)
        output.file.extend(external.file)
        output.timeseries.extend(external.timeseries)
        output.sequence.extend(external.sequence)

        node = dm.NodeApply(
            space=OMNI_SDK.instance_space,
            external_id=f"{view.external_id}:{faker.unique.first_name()}",
            sources=[
                dm.NodeOrEdgeData(
                    source=view.as_id(),
                    properties=properties,
                )
            ],
            type=node_type,
        )
        output.node.append(node)

    return output


def generate_mock_values(
    properties: dict[str, dm.MappedProperty], faker: Faker
) -> tuple[dict[str, PropertyValue], Data]:
    output = {}
    external = Data()
    for name, mapped in properties.items():
        if mapped.nullable and random.random() < 0.5:
            output[name] = None
            continue

        if isinstance(mapped.type, ListablePropertyType) and mapped.type.is_list:
            output[name] = [create_value(mapped.type, faker) for _ in range(random.randint(0, 5))]
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
                        )
                    )
                elif isinstance(mapped.type, dm.SequenceReference):
                    external.sequence.append(
                        Sequence(
                            external_id=ref,
                            name=ref,
                            columns=[SequenceColumn(external_id="value")],
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
    elif isinstance(prop, (dm.Int32, dm.Int64)):
        return random.randint(0, 100)
    elif isinstance(prop, (dm.Float32, dm.Float64)):
        return random.random() * 100
    elif isinstance(prop, dm.Boolean):
        return random.random() < 0.5
    elif isinstance(prop, dm.Json):
        return {create_value(dm.Text(), faker): create_value(dm.Text(), faker) for _ in range(random.randint(0, 5))}
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
