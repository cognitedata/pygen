from __future__ import annotations

import types
from abc import abstractmethod
from collections import UserList
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Generic, List, Optional, TypeVar, Union

import pandas as pd
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Properties, PropertyValue
from pydantic import BaseModel, ConfigDict, Extra, constr

# Todo - Move into SDK


@dataclass
class InstancesApply:
    """This represents the read result of an instance query

    Args:
        nodes (dm.NodeApplyList): A list of nodes.
        edges (dm.EdgeApply): A list of edges.

    """

    nodes: list[dm.NodeApply]
    edges: list[dm.EdgeApply]


ExternalId = constr(min_length=1, max_length=255)


class DomainModelCore(BaseModel):
    space: ClassVar[str]
    external_id: ExternalId

    def id_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id


class DomainModel(DomainModelCore):
    version: int
    last_updated_time: datetime
    created_time: datetime
    deleted_time: Optional[datetime] = None

    @classmethod
    def from_node(cls, node: dm.Node) -> T_TypeNode:
        data = node.dump(camel_case=False)

        return cls(**data, **unpack_properties(node.properties))

    @classmethod
    def one_to_many_fields(cls) -> list[str]:
        return [
            field_name
            for field_name, field in cls.model_fields.items()
            if isinstance(field.annotation, types.GenericAlias)
        ]


T_TypeNode = TypeVar("T_TypeNode", bound=DomainModel)


class DomainModelApply(DomainModelCore):
    model_config = ConfigDict(extra=Extra.forbid)
    existing_version: Optional[int] = None

    def to_instances_apply(self) -> InstancesApply:
        return self._to_instances_apply(set())

    @abstractmethod
    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        raise NotImplementedError()


class DomainModelApplyResult(DomainModelCore):
    version: int
    was_modified: bool
    last_updated_time: datetime
    created_time: datetime


class DataPoint(BaseModel):
    timestamp: str


class NumericDataPoint(DataPoint):
    value: float


class StringDataPoint(DataPoint):
    value: str


class TimeSeries(DomainModelCore):
    id: Optional[int] = None
    name: Optional[str] = None
    is_string: bool = False
    metadata: dict = {}
    unit: Optional[str] = None
    asset_id: Optional[int] = None
    is_step: bool = False
    description: Optional[str] = None
    security_categories: Optional[str] = None
    dataset_id: Optional[int] = None
    data_points: Union[List[NumericDataPoint], List[StringDataPoint]]


class TypeList(UserList, Generic[T_TypeNode]):
    _NODE: type[T_TypeNode]

    def __init__(self, nodes: Collection[type[DomainModelCore]]):
        # if any(not isinstance(node, self._NODE) for node in nodes):
        # raise TypeError(
        #     f"All nodes for class {type(self).__name__} must be of type " f"{type(self._NODE).__name__}."
        # )
        super().__init__(nodes)

    def dump(self) -> list[dict[str, Any]]:
        return [node.dict() for node in self.data]

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(self.dump())

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()


T_TypeApplyNode = TypeVar("T_TypeApplyNode", bound=DomainModelApply)
T_TypeNodeList = TypeVar("T_TypeNodeList", bound=TypeList)


class Identifier(BaseModel):
    _instance_type: ClassVar[str] = "node"
    space: constr(min_length=1, max_length=255)
    external_id: constr(min_length=1, max_length=255)

    @classmethod
    def from_direct_relation(cls, relation: dm.DirectRelationReference) -> T_Identifier:
        return cls(space=relation.space, external_id=relation.external_id)

    def __str__(self):
        return f"{self.space}/{self.external_id}"


T_Identifier = TypeVar("T_Identifier", bound=Identifier)


def unpack_properties(properties: Properties) -> dict[str, PropertyValue]:
    unpacked = {}
    for view_properties in properties.values():
        for prop_name, prop_value in view_properties.items():
            if isinstance(prop_value, (str, int, float, bool, list)):
                unpacked[prop_name] = prop_value
            elif isinstance(prop_value, dict):
                # Dicts are assumed to be reference properties
                if "space" in prop_value and "externalId" in prop_value:
                    unpacked[prop_name] = prop_value["externalId"]
                else:
                    raise ValueError(f"Unexpected reference property {prop_value}")
            else:
                raise ValueError(f"Unexpected property value type {type(prop_value)}")
    return unpacked
