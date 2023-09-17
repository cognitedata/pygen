from __future__ import annotations

import datetime
import types
from abc import abstractmethod
from collections import UserList
from collections.abc import Collection, Mapping
from typing import Any, ClassVar, Generic, Optional, TypeVar, Union

import pandas as pd
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Properties, PropertyValue
from pydantic import BaseModel, ConfigDict, Extra, Field


class DomainModelCore(BaseModel):
    space: ClassVar[str]
    external_id: str = Field(min_length=1, max_length=255)

    def id_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id


class DomainModel(DomainModelCore):
    version: int
    last_updated_time: datetime.datetime
    created_time: datetime.datetime
    deleted_time: Optional[datetime.datetime] = None

    @classmethod
    def from_node(cls: type[T_TypeNode], node: dm.Node) -> T_TypeNode:
        data = node.dump(camel_case=False)

        return cls(**data, **unpack_properties(node.properties))  # type: ignore[arg-type]

    @classmethod
    def one_to_many_fields(cls) -> list[str]:
        return [
            field_name
            for field_name, field in cls.model_fields.items()
            if isinstance(field.annotation, types.GenericAlias)
        ]


T_TypeNode = TypeVar("T_TypeNode", bound=DomainModel)


class DomainModelApply(DomainModelCore):
    model_config: ClassVar[ConfigDict] = ConfigDict(extra=Extra.forbid)
    existing_version: Optional[int] = None

    def to_instances_apply(self) -> dm.InstancesApply:
        return self._to_instances_apply(set())

    @abstractmethod
    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        raise NotImplementedError()


class DomainModelApplyResult(DomainModelCore):
    version: int
    was_modified: bool
    last_updated_time: datetime.datetime
    created_time: datetime.datetime


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
    metadata: dict[str, str] = Field(default_factory=dict)
    unit: Optional[str] = None
    asset_id: Optional[int] = None
    is_step: bool = False
    description: Optional[str] = None
    security_categories: Optional[str] = None
    dataset_id: Optional[int] = None
    data_points: Union[list[NumericDataPoint], list[StringDataPoint]]


class TypeList(UserList, Generic[T_TypeNode]):
    _NODE: type[T_TypeNode]

    def __init__(self, nodes: Collection[type[DomainModelCore]]):
        super().__init__(nodes)

    def dump(self) -> list[dict[str, Any]]:
        return [node.model_dump() for node in self.data]

    def to_pandas(self) -> pd.DataFrame:
        df = pd.DataFrame(self.dump())
        if df.empty:
            df = pd.DataFrame(columns=self._NODE.model_fields)

        # Reorder columns to have the custom columns first
        fixed_columns = set(DomainModel.model_fields)
        columns = (
            ["external_id"]
            + [col for col in df if col not in fixed_columns]
            + [col for col in DomainModel.model_fields if col != "external_id"]
        )
        return df[columns]

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()  # type: ignore[operator]


T_TypeApplyNode = TypeVar("T_TypeApplyNode", bound=DomainModelApply)
T_TypeNodeList = TypeVar("T_TypeNodeList", bound=TypeList)


def unpack_properties(properties: Properties) -> Mapping[str, PropertyValue]:
    unpacked: dict[str, PropertyValue] = {}
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
