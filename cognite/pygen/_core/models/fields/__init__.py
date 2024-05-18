"""Fields represents the fields of the data classes in the generated SDK."""

from __future__ import annotations

from .base import Field, T_Field
from .cdf_reference import CDFExternalField, CDFExternalListField
from .connections import (
    BaseConnectionField,
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
    OneToManyConnectionField,
    OneToOneConnectionField,
)
from .container import BaseContainerField, ContainerField, ContainerListField

__all__ = [
    "Field",
    "BaseContainerField",
    "ContainerField",
    "ContainerListField",
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
    "BaseConnectionField",
    "OneToOneConnectionField",
    "OneToManyConnectionField",
]
