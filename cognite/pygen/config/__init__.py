from dataclasses import dataclass
from dataclasses import field as dataclass_field

from .list_method import ListMethodFilters
from .naming import (
    APIClassNaming,
    Case,
    DataClassNaming,
    FieldNaming,
    MultiAPIClassNaming,
    Naming,
    NamingConfig,
    Number,
)

__all__ = [
    "PygenConfig",
    "NamingConfig",
    "MultiAPIClassNaming",
    "APIClassNaming",
    "DataClassNaming",
    "FieldNaming",
    "Naming",
    "Number",
    "Case",
    "ListMethodFilters",
]


@dataclass
class PygenConfig:
    """
    Configuration for how pygen should generate the SDK.

    Args:
        naming: The naming convention used by pygen.
    """

    naming: NamingConfig = dataclass_field(default_factory=NamingConfig)
    list_method: ListMethodFilters = dataclass_field(default_factory=ListMethodFilters)
