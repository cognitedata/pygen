"""This module contains functions for validating the uniqueness of names in the generated classes."""

from collections import defaultdict
from typing import Any, cast

from cognite.client import data_modeling as dm

from cognite.pygen.exceptions import NameConflict

from .models import APIClass, DataClass, MultiAPIClass

_DATACLASS_UNIQUE_PROPERTIES = [
    "file_name",
    "read_name",
    "write_name",
    "read_list_name",
    "write_list_name",
]

_APICLASS_UNIQUE_PROPERTIES = ["file_name", "name", "parent_attribute"]

_MULTIAPICLASS_UNIQUE_PROPERTIES = ["name"]


def validate_data_classes_unique_name(data_classes: list[DataClass]) -> None:
    """Checks that fields that are used in shared namespaces the data classes have unique names."""
    _validate(data_classes, _DATACLASS_UNIQUE_PROPERTIES, "view_id", DataClass.__name__)


def validate_api_classes_unique_names(api_classes: list[APIClass]) -> None:
    """Checks that fields that are used in shared namespaces the API classes have unique names."""
    _validate(api_classes, _APICLASS_UNIQUE_PROPERTIES, "view_id", APIClass.__name__)


def validate_multi_api_classes_unique_names(multi_api_classes: list[MultiAPIClass]) -> None:
    """Checks that fields that are used in shared namespaces the MultiAPI classes have unique names."""
    _validate(multi_api_classes, _MULTIAPICLASS_UNIQUE_PROPERTIES, "model_id", MultiAPIClass.__name__)


def _validate(items: list[Any], attributes: list[str], id_attribute: str, class_name: str) -> None:
    """Check that the given attributes are unique for each item in the list"""
    name_conflicts: list[tuple[str, list[dm.VersionedDataModelingId]]] = []
    for prop in attributes:
        ids_by_value = defaultdict(list)
        for item in items:
            value = cast(str, getattr(item, prop))
            id_ = cast(dm.VersionedDataModelingId, getattr(item, id_attribute))
            ids_by_value[value].append(id_)
        for prop, view_ids in ids_by_value.items():
            if len(view_ids) > 1:
                name_conflicts.append((prop, view_ids))
    if name_conflicts:
        raise NameConflict(name_conflicts, class_name)
