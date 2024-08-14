from __future__ import annotations

from itertools import chain
from unittest.mock import MagicMock

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.models import (
    EndNodeField,
    Field,
    NodeDataClass,
)
from cognite.pygen._core.models.fields import EdgeClass
from cognite.pygen.config import PygenConfig
from cognite.pygen.warnings import (
    ViewPropertyNameCollisionWarning,
)
from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni.data_classes import DomainModel, DomainModelWrite
else:
    from omni_pydantic_v1.data_classes import DomainModel, DomainModelWrite


@pytest.mark.parametrize(
    "name, expected_name",
    [
        ("property", "property_"),
        ("version", "version_"),
        ("yield", "yield_"),
        ("len", "len_"),
        ("def", "def_"),
        *{
            (name, f"{name.casefold()}_")
            for name in chain(dir(DomainModel), dir(DomainModelWrite))
            if not name.startswith("_")
        },
    ],
)
def test_field_from_property_expect_warning(name: str, expected_name, pygen_config: PygenConfig) -> None:
    # Arrange
    prop = dm.MappedProperty(
        container=dm.ContainerId("dummy", "dummy"),
        container_property_identifier=name,
        type=dm.Text(),
        nullable=True,
        immutable=False,
        auto_increment=False,
        name=name,
    )

    # Act
    with pytest.warns(ViewPropertyNameCollisionWarning):
        actual = Field.from_property(name, prop, {}, {}, pygen_config, dm.ViewId("a", "b", "c"), pydantic_field="Field")

    # Assert
    assert actual.name == expected_name


def field_type_hints_test_cases():
    site_apply_edge = MagicMock(spec=EdgeClass)
    site_apply1 = MagicMock(spec=NodeDataClass)
    site_apply1.read_name = "Site"
    site_apply1.write_name = "SiteApply"
    site_apply1.is_writable = True
    site_apply_edge.end_class = site_apply1
    site_apply_edge.used_directions = {"outwards"}

    site_apply_edge2 = MagicMock(spec=EdgeClass)
    site_apply2 = MagicMock(spec=NodeDataClass)
    site_apply2.read_name = "Site"
    site_apply2.write_name = "SiteApply"
    site_apply2.is_writable = True
    site_apply_edge2.end_class = site_apply2
    site_apply_edge2.used_directions = {"outwards"}

    field = EndNodeField(
        name="end_node",
        doc_name="end node",
        prop_name="end_node",
        description=None,
        pydantic_field="Field",
        edge_classes=[site_apply_edge, site_apply_edge2],
    )
    yield pytest.param(
        field,
        "Union[Site, str, dm.NodeId]",
        "Union[SiteApply, str, dm.NodeId]",
        id="EdgeOneToEndNode",
    )


@pytest.mark.parametrize("field,expected_read_hint, expected_write_hint", list(field_type_hints_test_cases()))
def test_fields_type_hints(field: Field, expected_read_hint: str, expected_write_hint: str) -> None:
    assert field.as_write_type_hint() == expected_write_hint
    assert field.as_read_type_hint() == expected_read_hint
