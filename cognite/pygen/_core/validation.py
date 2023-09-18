import warnings
from collections import defaultdict
from typing import Any, cast

from cognite.client import data_modeling as dm

from cognite.pygen.exceptions import NameConflict, ReservedWordConflict

from .data_classes import APIClass, DataClass, MultiAPIClass

_DATACLASS_UNIQUE_PROPERTIES = [
    "file_name",
    "read_name",
    "write_name",
    "read_list_name",
    "write_list_name",
]

_APICLASS_UNIQUE_PROPERTIES = ["file_name", "name", "client_attribute"]

_MULTIAPICLASS_UNIQUE_PROPERTIES = ["name", "client_attribute"]


def validate_data_classes(data_classes: list[DataClass]) -> None:
    _validate(data_classes, _DATACLASS_UNIQUE_PROPERTIES, "view_id", DataClass.__name__)
    classes_with_version = [
        data_class for data_class in data_classes if any(field.prop_name == "version" for field in data_class)
    ]
    if len(classes_with_version) > 1:
        warnings.warn(
            f"The dataclasses: {classes_with_version}, this field will overwrite the node.version thus making "
            f"it unavailable in the generated SDK.",
            stacklevel=2,
        )
    if conflicts := [data_class.view_id for data_class in data_classes if data_class.view_name == "core"]:
        raise ReservedWordConflict(conflicts, "core")


def validate_api_classes(api_classes: list[APIClass]) -> None:
    _validate(api_classes, _APICLASS_UNIQUE_PROPERTIES, "view_id", APIClass.__name__)


def validate_multi_api_classes(multi_api_classes: list[MultiAPIClass]) -> None:
    _validate(multi_api_classes, _MULTIAPICLASS_UNIQUE_PROPERTIES, "model_id", MultiAPIClass.__name__)


def _validate(items: list[Any], attributes: list[str], id_attribute: str, class_name: str) -> None:
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
