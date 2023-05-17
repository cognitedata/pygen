from __future__ import annotations

from typing import Sequence, Type, overload

from .core_list import T_TypeNode, T_TypeNodeList


class TypeAPI:
    def __init__(self, class_type: Type[T_TypeNode], class_list: Type[T_TypeNodeList], data: list[T_TypeNode] = None):
        self.class_type = class_type
        self.class_list = class_list
        self._data = data

    def list(self, propagation_limit: int = 0, limit: int = 25) -> T_TypeNodeList:
        return self.class_list(self._data)

    def apply(self, node: T_TypeNode, propagation_limit: int = 0):
        ...

    @overload
    def retrieve(self, external_id: str, propagation_limit: int = 0) -> T_TypeNode:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str], propagation_limit: int = 0) -> T_TypeNodeList:
        ...

    def retrieve(self, external_id: str | Sequence[str], propagation_limit: int = 0) -> T_TypeNode | T_TypeNodeList:
        ...

    def delete(self, node_external_id: str | T_TypeNode | T_TypeNodeList, propagation_limit: int = 0):
        ...
