from __future__ import annotations

from collections import UserList
from typing import Any, Collection, Type, TypeVar

import pandas as pd

from .core import DomainModelApply, DomainModelCore, T_TypeNode


class TypeList(UserList):
    _NODE: Type[T_TypeNode]

    def __init__(self, nodes: Collection[Type[DomainModelCore]]):
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
