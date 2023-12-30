from __future__ import annotations

import datetime
from typing import Optional

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Properties
from pydantic import Field

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni.data_classes._core import DomainModel, unpack_properties
else:
    from omni_pydantic_v1.data_classes._core import DomainModel, unpack_properties


class TestDomainModel:
    def test_repr(self):
        # Arrange
        class Foo(DomainModel):
            space: str = "FooSpace"
            bar: Optional[Bar] = Field(None, repr=False)

        class Bar(DomainModel):
            space: str = "BarSpace"
            foo: Optional[Foo] = Field(None, repr=False)

        try:
            Foo.model_rebuild()
            Bar.model_rebuild()
        except AttributeError as e:
            if "has no attribute 'model_rebuild" in str(e):
                # is pydantic v1
                Foo.update_forward_refs(Bar=Bar)
                Bar.update_forward_refs(Foo=Foo)
            else:
                raise e

        foo = Foo(
            external_id="foo",
            version=1,
            created_time=datetime.datetime(2023, 1, 1),
            last_updated_time=datetime.datetime(2024, 1, 1),
        )
        bar = Bar(
            external_id="bar",
            version=1,
            created_time=datetime.datetime(2023, 1, 1),
            last_updated_time=datetime.datetime(2024, 1, 1),
        )
        foo.bar = bar
        bar.foo = foo

        # Act
        foo_repr = repr(foo)
        bar_repr = repr(bar)

        # Assert
        assert (
            foo_repr == "Foo(space='FooSpace', external_id='foo', "
            "version=1, last_updated_time=datetime.datetime(2024, 1, 1, 0, 0), "
            "created_time=datetime.datetime(2023, 1, 1, 0, 0), deleted_time=None)"
        )
        assert (
            bar_repr == "Bar(space='BarSpace', external_id='bar', "
            "version=1, last_updated_time=datetime.datetime(2024, 1, 1, 0, 0), "
            "created_time=datetime.datetime(2023, 1, 1, 0, 0), deleted_time=None)"
        )


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
