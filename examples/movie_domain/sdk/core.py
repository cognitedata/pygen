from __future__ import annotations

import inspect
from collections import UserList
from typing import Any, Collection, ForwardRef, Mapping, Optional, Sequence, Type, TypeVar, overload

import pandas as pd
from pydantic import BaseModel, constr
from pydantic.utils import DUNDER_ATTRIBUTES


class DomainModel(BaseModel):
    external_id: Optional[constr(min_length=1, max_length=255)] = None


class CircularModel(DomainModel):
    def _domain_fields(self) -> set[str]:
        domain_fields = set()
        for field_name, field in self.__fields__.items():
            is_forward_ref = isinstance(field.type_, ForwardRef)
            is_domain = inspect.isclass(field.type_) and issubclass(field.type_, DomainModel)
            is_list_domain = (
                (not is_forward_ref)
                and field.sub_fields
                and any(issubclass(sub.type_, DomainModel) for sub in field.sub_fields)
            )
            if is_forward_ref or is_domain or is_list_domain:
                domain_fields.add(field_name)
        return domain_fields

    def dict(
        self,
        *,
        include: Optional[set[int | str] | Mapping[int | str, Any]] = None,
        exclude: Optional[Any] = None,
        by_alias: bool = False,
        skip_defaults: Optional[bool] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> dict[str, any]:
        exclude = exclude or set()
        domain_fields = self._domain_fields()

        return super().dict(
            include=include,
            exclude=exclude | domain_fields,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )

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


class TimeSeries(DomainModel):
    name: str


class TypeList(UserList):
    _NODE: Type[DomainModel]

    def __init__(self, nodes: Collection[Type[DomainModel]]):
        if any(not isinstance(node, self._NODE) for node in nodes):
            raise TypeError(
                f"All nodes for class {type(self).__name__} must be of type " f"{type(self._NODE).__name__}."
            )
        super().__init__(nodes)

    def dump(self) -> list[dict[str, Any]]:
        return [node.dict(exclude_unset=True) for node in self.data]

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(self.dump())

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()


T_TypeNode = TypeVar("T_TypeNode", bound=DomainModel)
T_TypeNodeList = TypeVar("T_TypeNodeList", bound=TypeList)


class TypeAPI:
    def __init__(self, class_type: Type[T_TypeNode], class_list: Type[T_TypeNodeList]):
        self.class_type = class_type
        self.class_list = class_list

    def list(self, limit: int) -> T_TypeNodeList:
        ...

    def apply(self, node: T_TypeNode, propagation_limit: int = 1):
        ...

    @overload
    def retrieve(self, external_id: str) -> T_TypeNode:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> T_TypeNodeList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> T_TypeNode | TypeList:
        ...

    def delete(self, node_external_id: str | T_TypeNode | T_TypeNodeList, propagation_limit: int = 0):
        ...
