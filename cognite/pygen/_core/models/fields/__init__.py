"""Fields represents the fields of the data classes in the generated SDK."""

from __future__ import annotations

from .base import Field, T_Field
from .cdf_reference import CDFExternalField, CDFExternalListField
from .connections import (
    EdgeClasses,
    EdgeField,
    EdgeOneToEndNode,
    EdgeOneToMany,
    EdgeOneToManyEdges,
    EdgeOneToManyNodes,
    EdgeOneToOne,
    EdgeOneToOneAny,
    EdgeToOneDataClass,
    EdgeTypedOneToOne,
)
from .primitive import ListFieldCore, PrimitiveField, PrimitiveFieldCore, PrimitiveListField

__all__ = [
    "Field",
    "PrimitiveFieldCore",
    "PrimitiveField",
    "PrimitiveListField",
    "ListFieldCore",
    "CDFExternalField",
    "CDFExternalListField",
    "EdgeField",
    "EdgeOneToManyEdges",
    "EdgeOneToManyNodes",
    "EdgeToOneDataClass",
    "EdgeOneToOne",
    "EdgeOneToMany",
    "EdgeOneToEndNode",
    "EdgeTypedOneToOne",
    "EdgeClasses",
    "T_Field",
    "EdgeTypedOneToOne",
    "EdgeOneToOneAny",
]
