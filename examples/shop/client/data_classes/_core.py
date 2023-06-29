from __future__ import annotations

import inspect
import types
from abc import abstractmethod
from collections import UserList
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, ForwardRef, Generic, List, Optional, TypeVar, Union

import pandas as pd
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Properties, PropertyValue
from pydantic import BaseModel, Extra, constr
from pydantic.utils import DUNDER_ATTRIBUTES

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
    version: str
    last_updated_time: datetime
    created_time: datetime
    deleted_time: Optional[datetime]

    @classmethod
    def from_node(cls, node: dm.Node) -> T_TypeNode:
        data = node.dump(camel_case=False)

        return cls(**data, **unpack_properties(node.properties))

    @classmethod
    def one_to_many_fields(cls) -> list[str]:
        return [
            field_name
            for field_name, field in cls.__fields__.items()
            if isinstance(field.outer_type_, types.GenericAlias)
        ]


T_TypeNode = TypeVar("T_TypeNode", bound=DomainModel)


class DomainModelApply(DomainModelCore):
    existing_version: Optional[int] = None

    def to_instances_apply(self) -> InstancesApply:
        return self._to_instances_apply(set())

    @abstractmethod
    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        raise NotImplementedError()

    class Config:
        extra = Extra.forbid


class DomainModelApplyResult(DomainModelCore):
    version: str
    was_modified: bool
    last_updated_time: datetime
    created_time: datetime


def _is_subclass(class_type: Any, _class: Any) -> bool:
    return inspect.isclass(class_type) and issubclass(class_type, _class)


class CircularModelCore(DomainModelCore):
    def _domain_fields(self) -> set[str]:
        domain_fields = set()
        for field_name, field in self.__fields__.items():
            is_forward_ref = isinstance(field.type_, ForwardRef)
            is_domain = _is_subclass(field.type_, DomainModelCore)
            is_list_domain = (
                (not is_forward_ref)
                and field.sub_fields
                and any(_is_subclass(sub.type_, DomainModelCore) for sub in field.sub_fields)
            )
            is_list_forward_ref = field.sub_fields and any(
                isinstance(sub.type_, ForwardRef) for sub in field.sub_fields
            )
            if is_forward_ref or is_domain or is_list_domain or is_list_forward_ref:
                domain_fields.add(field_name)
        return domain_fields

    def _iter(
        self,
        to_dict: bool = False,
        by_alias: bool = False,
        include: set[int | str] | Mapping[int | str, Any] | None = None,
        exclude: set[int | str] | Mapping[int | str, Any] | None = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> Iterable[tuple]:
        domain_fields = self._domain_fields()
        yield from super()._iter(
            to_dict,
            by_alias,
            include,
            (exclude or set()) | domain_fields,
            exclude_unset,
            exclude_defaults,
            exclude_none,
        )
        for field in domain_fields:
            # yield field, None
            if value := getattr(self, field):
                if isinstance(value, list):
                    yield field, [v.external_id if hasattr(v, "external_id") else v for v in value]
                else:
                    yield field, value.external_id if hasattr(value, "external_id") else value
            else:
                yield field, None

    def __repr_args__(self) -> Sequence[tuple[str | None, Any]]:
        """
        This is overwritten to avoid an infinite recursion when calling str, repr, or pretty
        on the class object.
        """
        domain_fields = self._domain_fields()
        output = []
        for k, v in self.__dict__.items():
            if k not in DUNDER_ATTRIBUTES and (k not in self.__fields__ or self.__fields__[k].field_info.repr):
                if k not in domain_fields:
                    output.append((k, v))
                    continue

                if isinstance(v, list):
                    output.append((k, [x.external_id if hasattr(x, "external_id") else None for x in v]))
                elif hasattr(v, "external_id"):
                    output.append((k, v.external_id))
        return output

    def traverse(self, depth: int = 0, tmp_cache: dict[str, Any] = None):
        tmp_cache = tmp_cache or {}
        if self.external_id in tmp_cache:
            return tmp_cache[self.external_id]

        tmp_cache[self.external_id] = self.copy()
        if depth == 0:
            return tmp_cache[self.external_id]

        for domain_field in self._domain_fields():
            value = getattr(self, domain_field)
            if value is None:
                value = None
            elif isinstance(value, list):
                value = [entry.traverse(depth=depth - 1, tmp_cache=tmp_cache) for entry in value]
            else:
                value = (
                    value.traverse(depth=depth - 1, tmp_cache=tmp_cache) if hasattr(value, "traverse") else value.copy()
                )
            setattr(tmp_cache[self.external_id], domain_field, value)

        return tmp_cache[self.external_id]


class CircularModel(CircularModelCore, DomainModel):
    ...


class CircularModelApply(CircularModelCore, DomainModelApply):
    ...


class DataPoint(BaseModel):
    timestamp: str


class NumericDataPoint(DataPoint):
    value: float


class StringDataPoint(DataPoint):
    value: str


class TimeSeries(DomainModelCore):
    id: Optional[int]
    name: Optional[str]
    is_string: bool = False
    metadata: dict = {}
    unit: Optional[str]
    asset_id: Optional[int]
    is_step: bool = False
    description: Optional[str]
    security_categories: Optional[str]
    dataset_id: Optional[int]
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
