from __future__ import annotations

from collections import UserList
from typing import Any, Collection, Type, TypeVar

import pandas as pd

from .core import DomainModel


class TypeList(UserList):
    _NODE: Type[DomainModel]

    def __init__(self, nodes: Collection[Type[DomainModel]]):
        # if any(not isinstance(node, self._NODE) for node in nodes):
        # raise TypeError(
        #     f"All nodes for class {type(self).__name__} must be of type " f"{type(self._NODE).__name__}."
        # )
        super().__init__(nodes)

    def dump(self) -> list[dict[str, Any]]:
        return [node.dict(exclude_unset=True) for node in self.data]

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(self.dump())

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()


T_TypeNode = TypeVar("T_TypeNode", bound=DomainModel)
T_TypeNodeList = TypeVar("T_TypeNodeList", bound=TypeList)
