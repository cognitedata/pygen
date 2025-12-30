"""Example SDK for the pygen example data model.

This module provides an example implementation of a generated SDK that demonstrates
how to use the generic InstanceClient and InstanceAPI classes to create type-safe
clients for specific CDF Data Models.

The example data model includes:
- ProductNode: Node view with various property types (text, float, int, bool, date, datetime)
  and a direct relation to CategoryNode
- CategoryNode: Node view with a reverse direct relation to ProductNode
- RelatesTo: Edge view for relating nodes with properties
"""

from ._client import ExampleClient
from ._data_class import (
    CategoryNode,
    CategoryNodeList,
    CategoryNodeWrite,
    ProductNode,
    ProductNodeList,
    ProductNodeWrite,
    RelatesTo,
    RelatesToList,
    RelatesToWrite,
)

__all__ = [
    "CategoryNode",
    "CategoryNodeList",
    "CategoryNodeWrite",
    "ExampleClient",
    "ProductNode",
    "ProductNodeList",
    "ProductNodeWrite",
    "RelatesTo",
    "RelatesToList",
    "RelatesToWrite",
]
