"""Reserved words that cannot be used as identifiers.

These sets define names that conflict with Python, Pydantic, or
the generated SDK infrastructure.
"""

import builtins
import keyword

from pydantic import BaseModel

# Python language reserved
_PYTHON_KEYWORDS = set(keyword.kwlist)
_PYTHON_BUILTINS = {name for name in vars(builtins) if not name.startswith("_")}

# Pydantic BaseModel methods and attributes
_PYDANTIC_BASEMODEL = {name for name in dir(BaseModel) if not name.startswith("_")}

# SDK infrastructure - fields on generated data classes
_SDK_DATA_CLASS_FIELDS = {
    "space",
    "external_id",
    "version",
    "created_time",
    "last_updated_time",
    "deleted_time",
    "existing_version",
    "type",
    "data_record",
    "node_type",
}

# SDK infrastructure - methods on generated data classes
_SDK_DATA_CLASS_METHODS = {
    "as_id",
    "as_tuple_id",
    "as_direct_reference",
    "to_pandas",
    "to_instances_write",
    "to_instances_apply",
    "from_instance",
    "dump",
}

# SDK infrastructure - reserved parameter names in filter/list methods
_SDK_PARAMETERS = {
    "limit",
    "filter",
    "sort",
    "retrieve_connections",
    "space",
}

# Reserved data class names
_SDK_CLASS_NAMES = {
    "DomainModel",
    "DomainModelWrite",
    "DomainModelList",
    "DomainModelWriteList",
}

# Reserved file names
_SDK_FILE_NAMES = {
    "__init__",
    "_core",
    "_api",
}

# Combined sets for each context
FIELD_RESERVED = (
    _PYTHON_KEYWORDS | _PYTHON_BUILTINS | _PYDANTIC_BASEMODEL | _SDK_DATA_CLASS_FIELDS | _SDK_DATA_CLASS_METHODS
)

PARAMETER_RESERVED = _PYTHON_KEYWORDS | _PYTHON_BUILTINS | _SDK_PARAMETERS

DATA_CLASS_RESERVED = _PYTHON_KEYWORDS | _PYTHON_BUILTINS | _SDK_CLASS_NAMES

FILE_RESERVED = _SDK_FILE_NAMES


def is_reserved(name: str, context: str) -> tuple[bool, str]:
    """Check if a name is reserved in the given context.

    Args:
        name: The name to check.
        context: One of "field", "parameter", "class", "file".

    Returns:
        Tuple of (is_reserved, reason). If reserved, reason explains why.
    """
    if keyword.iskeyword(name):
        return True, "Python keyword"

    if name in _PYTHON_BUILTINS:
        return True, "Python builtin"

    if context == "field":
        if name in _PYDANTIC_BASEMODEL:
            return True, "Pydantic BaseModel attribute"
        if name in _SDK_DATA_CLASS_FIELDS:
            return True, "SDK data class field"
        if name in _SDK_DATA_CLASS_METHODS:
            return True, "SDK data class method"

    elif context == "parameter":
        if name in _SDK_PARAMETERS:
            return True, "SDK parameter"

    elif context == "class":
        if name in _SDK_CLASS_NAMES:
            return True, "SDK class name"

    elif context == "file":
        if name in _SDK_FILE_NAMES:
            return True, "SDK file name"

    return False, ""
