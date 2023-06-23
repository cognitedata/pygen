from __future__ import annotations

import inspect
from datetime import datetime
from typing import Any, ForwardRef, Iterable, Mapping, Optional, Sequence, TypeVar, Union

from cognite.client import data_modeling as dm
from pydantic import BaseModel, constr
from pydantic.utils import DUNDER_ATTRIBUTES


class DomainModelCore(BaseModel):
    space: constr(min_length=1, max_length=255)
    external_id: constr(min_length=1, max_length=255)


class DomainModel(DomainModelCore):
    version: str
    last_updated_time: datetime
    created_time: datetime
    deleted_time: Optional[datetime]

    @classmethod
    def from_node(cls, node: dm.Node) -> T_TypeNode:
        data = node.dump(camel_case=False)
        return cls(**data, **{k: v for prop in node.properties.values() for k, v in prop.items()})


T_TypeNode = TypeVar("T_TypeNode", bound=DomainModel)


class DomainModelApply(DomainModelCore):
    existing_version: int


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
        include: Optional[set[int | str] | Mapping[int | str, Any]] = None,
        exclude: Optional[set[int | str] | Mapping[int | str, Any]] = None,
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
            yield field, None
            # if value := getattr(self, field):
            #     if isinstance(value, list):
            #         yield field, [v.external_id if hasattr(v, "external_id") else v for v in value]
            #     else:
            #         yield field, value.external_id if hasattr(value, "external_id") else value
            # else:
            #     yield field, None

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
    data_points: Union[list[NumericDataPoint], list[StringDataPoint]]
