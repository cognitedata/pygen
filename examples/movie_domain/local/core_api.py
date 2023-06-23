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
        is_singular = isinstance(external_id, str)
        id_set = {external_id} if is_singular else set(external_id)
        selected_nodes = [node.copy() for node in self._data if node.externalId in id_set]
        return selected_nodes[0] if is_singular else self.class_list(selected_nodes)

    def delete(self, node_external_id: str | T_TypeNode | T_TypeNodeList, propagation_limit: int = 0):
        ...
