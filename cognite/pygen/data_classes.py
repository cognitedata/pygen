from __future__ import annotations

import graphlib
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import field as dfield

from cognite.pygen.dm_clients.misc import to_snake

BUILTIN_TYPES = {
    "str",
    "float",
    "int",
    "bool",
    "Timestamp",
    "JSONObject",
}


@dataclass
class Field:
    name: str
    type: str
    is_required: bool = False
    is_list: bool = False
    is_named_type: bool = False

    @property
    def type_hint(self) -> str:
        type_hint = self.type
        if self.is_list and not self.is_required:
            type_hint = f"Optional[List[Optional[{type_hint}]]] = []"
        elif not self.is_required:
            type_hint = f"Optional[{type_hint}] = None"
        elif self.is_list:
            type_hint = f"List[{type_hint}]"
        return type_hint

    def __repr__(self) -> str:
        return self.name


@dataclass
class DomainModel:
    name: str
    fields: list[Field]
    is_root_node: bool = dfield(init=False, default=False)

    @property
    def name_snake(self) -> str:
        return to_snake(self.name)

    @property
    def dependencies(self) -> set[str]:
        return {field.type for field in self.fields if field.type not in BUILTIN_TYPES}

    def __hash__(self):
        return hash(self.name)

    def __repr__(self) -> str:
        return f"{self.name}({', '.join(repr(field) for field in self.fields)})"


@dataclass
class DomainModels:
    models: list[DomainModel]

    @property
    def topological_order(self) -> list[DomainModel]:
        topological_order = graphlib.TopologicalSorter(
            {model.name: model.dependencies for model in self.models}
        ).static_order()
        return [next(model for model in self.models if model.name == model_name) for model_name in topological_order]

    def __iter__(self) -> Iterator[DomainModel]:
        return iter(self.models)
