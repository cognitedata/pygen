from __future__ import annotations

import inspect
from typing import Any, ForwardRef, Iterable, Mapping, Optional, Sequence

from pydantic import BaseModel, constr
from pydantic.utils import DUNDER_ATTRIBUTES


class DomainModel(BaseModel):
    external_id: Optional[constr(min_length=1, max_length=255)] = None


def _is_subclass(class_type: Any, _class: Any) -> bool:
    return inspect.isclass(class_type) and issubclass(class_type, _class)


class CircularModel(DomainModel):
    def _domain_fields(self) -> set[str]:
        domain_fields = set()
        for field_name, field in self.__fields__.items():
            is_forward_ref = isinstance(field.type_, ForwardRef)
            is_domain = _is_subclass(field.type_, DomainModel)
            is_list_domain = (
                (not is_forward_ref)
                and field.sub_fields
                and any(_is_subclass(sub.type_, DomainModel) for sub in field.sub_fields)
            )
            if is_forward_ref or is_domain or is_list_domain:
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
            to_dict, by_alias, include, exclude | domain_fields, exclude_unset, exclude_defaults, exclude_none
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
