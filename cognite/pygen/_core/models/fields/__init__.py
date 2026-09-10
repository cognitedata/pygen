"""Fields represents the fields of the data classes in the generated SDK."""

from __future__ import annotations

from .base import Field, T_Field
from .cdf_reference import CDFExternalField, CDFExternalListField
from .connections import (
    BaseConnectionField,
    EdgeClass,
    EndNodeField,
    OneToManyConnectionField,
    OneToOneConnectionField,
)
from .primitive import BasePrimitiveField, ContainerProperty, PrimitiveField, PrimitiveListField

__all__ = [
    "BaseConnectionField",
    "BasePrimitiveField",
    "CDFExternalField",
    "CDFExternalListField",
    "ContainerProperty",
    "EdgeClass",
    "EndNodeField",
    "Field",
    "OneToManyConnectionField",
    "OneToOneConnectionField",
    "PrimitiveField",
    "PrimitiveListField",
    "T_Field",
]
