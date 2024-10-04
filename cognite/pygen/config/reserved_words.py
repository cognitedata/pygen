from __future__ import annotations

import builtins
import keyword
from typing import Literal

from cognite.client import data_modeling as dm
from pydantic import BaseModel

from cognite.pygen.warnings import NameCollisionWarning

PYTHON_BUILTIN_NAMES = {name for name in vars(builtins) if not name.startswith("_")}
FIELD_NAMES = (
    {
        "space",
        "external_id",
        "version",
        "last_updated_time",
        "created_time",
        "deleted_time",
        "existing_version",
        "external_id_factory",
        "replace",
        "type",
        "list_full",
    }
    | {f for f in dir(BaseModel)}
    | {
        # Pydantic from DomainModel and DomainModelWrite
        "as_id",
        "data_record",
        "as_tuple_id",
        "as_direct_reference",
        "to_pandas",
        "_repr_html_",
        "external_id_factory",
        "to_instances_write",
        "_to_instances_write",
        "to_instances_apply",
        "_to_instances_apply",
        "from_instance",
        "config",
        "create_external_id_if_factory",
        "dump",
    }
)
PARAMETER_NAMES = {
    "interval",
    "limit",
    "filter",
    "replace",
    "retrieve_edges",
    "property",
}

DATA_CLASS_NAMES = {"DomainModel", "DomainModelWrite", "DomainModelList", "DomainModelWriteList"}
FILE_NAMES = {
    "__init__",
    "_core",
}


_NAMES_BY_TYPE = {
    "field": FIELD_NAMES,
    "parameter": PARAMETER_NAMES,
    "data class": DATA_CLASS_NAMES,
    "filename": FILE_NAMES,
}


def is_reserved_word(
    word: str,
    word_type: Literal["field", "data class", "parameter", "filename"],
    view_id: dm.ViewId | None = None,
    property_name: str | None = None,
) -> bool:
    if keyword.iskeyword(word) or word in PYTHON_BUILTIN_NAMES or word in _NAMES_BY_TYPE[word_type]:
        NameCollisionWarning.create(word, view_id, property_name).warn()
        return True
    return False
