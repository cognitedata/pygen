from __future__ import annotations

import datetime
from typing import Optional

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Properties
from omni import data_classes as dc
from omni.data_classes._core import DataRecord, DomainModel, unpack_properties
from pydantic import Field


class Foo(DomainModel):
    space: str = "FooSpace"
    bar: Optional[Bar] = Field(None, repr=False)


class Bar(DomainModel):
    space: str = "BarSpace"
    foo: Optional[Foo] = Field(None, repr=False)


class TestDomainModel:
    def test_repr(self):
        # Arrange
        Foo.model_rebuild()
        Bar.model_rebuild()

        foo = Foo(
            external_id="foo",
            data_record=DataRecord(
                version=1,
                created_time=datetime.datetime(2023, 1, 1),
                last_updated_time=datetime.datetime(2024, 1, 1),
            ),
        )
        bar = Bar(
            external_id="bar",
            data_record=DataRecord(
                version=1,
                created_time=datetime.datetime(2023, 1, 1),
                last_updated_time=datetime.datetime(2024, 1, 1),
            ),
        )
        foo.bar = bar
        bar.foo = foo

        # Act
        foo_repr = repr(foo)
        bar_repr = repr(bar)

        # Assert
        assert (
            foo_repr == "Foo(space='FooSpace', external_id='foo', "
            "data_record=DataRecord(version=1, last_updated_time=datetime.datetime(2024, 1, 1, 0, 0), "
            "created_time=datetime.datetime(2023, 1, 1, 0, 0), deleted_time=None), "
            "node_type=None)"
        )
        assert (
            bar_repr == "Bar(space='BarSpace', external_id='bar', "
            "data_record=DataRecord(version=1, last_updated_time=datetime.datetime(2024, 1, 1, 0, 0), "
            "created_time=datetime.datetime(2023, 1, 1, 0, 0), deleted_time=None), "
            "node_type=None)"
        )


class TestDomainModelWrite:
    def test_to_instances_write_with_allow_version_increase(self) -> None:
        # Arrange
        domain_node = dc.PrimitiveNullableWrite(
            external_id="1",
            data_record=dc.DataRecordWrite(existing_version=1),
            float_64=1.0,
        )

        # Act
        result = domain_node.to_instances_write(allow_version_increase=True)

        # Assert
        assert len(result.nodes) == 1
        node = result.nodes[0]
        assert node.external_id == "1"
        assert node.existing_version is None
        assert node.sources[0].properties == {"float64": 1.0}


def unpack_properties_test_cases():
    properties = {
        "IntegrationTestsImmutable": {
            "Person/2": {
                "name": "Christoph Waltz",
                "birthYear": 1956,
            }
        }
    }
    expected = {
        "name": "Christoph Waltz",
        "birthYear": 1956,
    }
    yield pytest.param(properties, expected, id="Person")

    properties = {
        "IntegrationTestsImmutable": {
            "Actor/2": {
                "person": {"space": "IntegrationTestsImmutable", "externalId": "person:ethan_coen"},
                "wonOscar": True,
            }
        }
    }
    expected = {"person": dm.NodeId("IntegrationTestsImmutable", "person:ethan_coen"), "wonOscar": True}
    yield pytest.param(properties, expected, id="Actor")


@pytest.mark.parametrize("raw_properties, expected", list(unpack_properties_test_cases()))
def test_unpack_properties(raw_properties: dict, expected: dict):
    # Arrange
    properties = Properties.load(raw_properties)

    # Act
    actual = unpack_properties(properties)

    # Assert
    assert actual == expected
