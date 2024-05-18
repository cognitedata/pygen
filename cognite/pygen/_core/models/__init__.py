"""This subpackage contains the classes that represent the internal representation of the API and data classes.
for the generated SDK.

They are made from the input views to pygen.
"""

from . import fields
from .api_classes import APIClass, MultiAPIClass
from .data_classes import DataClass, EdgeDataClass, NodeDataClass
from .fields import (
    BaseConnectionField,
    BasePrimitiveField,
    CDFExternalField,
    EdgeField,
    EdgeOneToEndNode,
    Field,
    OneToManyConnectionField,
    OneToOneConnectionField,
    PrimitiveField,
    PrimitiveListField,
)
from .filter_methods import FilterImplementation, FilterImplementationOnetoOneEdge, FilterMethod, FilterParameter

__all__ = [
    "fields",
    "DataClass",
    "APIClass",
    "MultiAPIClass",
    "NodeDataClass",
    "EdgeDataClass",
    "Field",
    "BasePrimitiveField",
    "PrimitiveField",
    "CDFExternalField",
    "EdgeField",
    "EdgeOneToEndNode",
    "FilterMethod",
    "FilterParameter",
    "FilterImplementation",
    "FilterImplementationOnetoOneEdge",
    "BaseConnectionField",
    "OneToOneConnectionField",
    "OneToManyConnectionField",
    "PrimitiveListField",
]
