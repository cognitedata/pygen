from __future__ import annotations

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.data_classes import Field, PrimitiveListField
from cognite.pygen.config import PygenConfig


def load_field_test_cases():
    raw_data = {
        "container": {"space": "power-ops", "externalId": "BenchmarkProcess"},
        "containerPropertyIdentifier": "runEvents",
        "type": {"list": True, "collation": "ucs_basic", "type": "text"},
        "nullable": True,
        "autoIncrement": False,
        "source": None,
        "defaultValue": None,
        "name": "runEvents",
        "description": None,
    }
    mapped = dm.MappedProperty.load(raw_data)
    yield pytest.param(
        mapped,
        PrimitiveListField(
            name="run_events",
            prop_name="runEvents",
            pydantic_field="Field",
            type_="str",
            is_nullable=True,
            prop=mapped,
        ),
        'list[str] = Field(default_factory=list, alias="runEvents")',
        "list[str] = []",
        id="PrimitiveListField that require alias.",
    )


@pytest.mark.parametrize("property_, expected, read_type_hint, write_type_hint", load_field_test_cases())
def test_load_field(
    property_: dm.MappedProperty | dm.ConnectionDefinition,
    expected: Field,
    read_type_hint: str,
    write_type_hint: str,
    pygen_config: PygenConfig,
) -> None:
    # Act
    actual = Field.from_property(property_.name, property_, {}, pygen_config.naming.field, view_name="dummy")

    # Assert
    assert actual == expected
    assert actual.as_write_type_hint() == write_type_hint
    assert actual.as_read_type_hint() == read_type_hint
