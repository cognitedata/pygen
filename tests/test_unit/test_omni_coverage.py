"""
The goal of the model Omni is to cover all property types of a view.
The test in this file is to ensure that the model Omni is up-to-date with the
possible properties of a view.
"""

import abc
from collections.abc import Iterable
from itertools import product
from typing import TypeVar

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.data_types import ListablePropertyType

NOT_SUPPORTED = {
    # Todo Add Omnium Connection to have support for direct nullable type.
    # type, listable, nullable
    ("direct", False, True),
    ("direct", True, True),
    ("direct", True, False),
    ("direct", False, False),
    ("timeseries", True, False),
    ("timeseries", False, False),
    ("file", True, False),
    ("file", False, False),
    ("sequence", True, False),
    ("sequence", False, False),
    ("enum", False, True),
    ("enum", True, True),
    ("enum", True, False),
    ("enum", False, False),
}


def test_mapped_properties_coverage(omni_data_model) -> None:
    existing_properties = existing_mapped_properties()
    covered_properties = properties_from_views(omni_data_model.views)

    missing = existing_properties - covered_properties
    assert not missing, f"Missing {len(missing)}:  {missing}"


T_Type = TypeVar("T_Type")


def properties_from_views(views: list[dm.View]) -> set[tuple[str, bool, bool]]:
    properties = set()
    for view in views:
        for prop in view.properties.values():
            if isinstance(prop, dm.MappedProperty) and isinstance(prop.type, ListablePropertyType):
                properties.add((prop.type._type, prop.type.is_list, prop.nullable))
    return properties


def _concrete_subclasses(cls: type[T_Type]) -> Iterable[type[T_Type]]:
    for sub in cls.__subclasses__():
        if abc.ABC not in sub.__bases__:
            yield sub
        yield from _concrete_subclasses(sub)


def existing_mapped_properties() -> set[tuple[str, bool, bool]]:
    types = list(_concrete_subclasses(dm.PropertyType))

    return {
        (t._type, listable, nullable) for t, listable, nullable in product(types, [True, False], [True, False])
    } - NOT_SUPPORTED
