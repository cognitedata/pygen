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
            to_dict,
            by_alias,
            include,
            (exclude or set()) | domain_fields,
            exclude_unset,
            exclude_defaults,
            exclude_none,
        )
        for field in domain_fields:
            if value := getattr(self, field):
                if isinstance(value, list):
                    yield field, [v.external_id if isinstance(v, DomainModel) else v for v in value]
                else:
                    yield field, value.external_id
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

    def traverse(self, depth: int = 0, tmp_cache: dict = None):
        return self._traverse(depth, tmp_cache or {})

    def _traverse(self, depth: int, cache: dict[str, Any]):
        if self.external_id in cache:
            return cache[self.external_id]

        cache[self.external_id] = self.copy()
        if depth == 0:
            return cache[self.external_id]

        for domain_field in self._domain_fields():
            value = getattr(self, domain_field)
            if value is None:
                value = None
            elif isinstance(value, list):
                value = [entry._traverse(depth=depth - 1, cache=cache) for entry in value]
            else:
                value = value._traverse(depth=depth - 1, cache=cache)
            setattr(cache[self.external_id], domain_field, value)

        return cache[self.external_id]


class TimeSeries(DomainModel):
    name: str
