from __future__ import annotations

from typing import Sequence, Type, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ..data_classes.core_list import T_TypeApplyNode, T_TypeNode, T_TypeNodeList


class TypeAPI:
    def __init__(
        self,
        client: CogniteClient,
        sources: dm.ViewIdentifier | Sequence[dm.ViewIdentifier] | dm.View | Sequence[dm.View],
        class_type: Type[T_TypeNode],
        class_apply_type: Type[T_TypeApplyNode],
        class_list: Type[T_TypeNodeList],
    ):
        self._client = client
        self.sources = sources
        self.class_type = class_type
        self.class_apply_type = class_apply_type
        self.class_list = class_list

    def apply(self, node: T_TypeNode, traversal_count: int = 0):
        raise NotImplementedError()

    @overload
    def retrieve(self, external_id: str, traversal_count: int = 0) -> T_TypeNode:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str], traversal_count: int = 0) -> T_TypeNodeList:
        ...

    def retrieve(self, external_id: str | Sequence[str], traversal_count: int = 0) -> T_TypeNode | T_TypeNodeList:
        raise NotImplementedError()

    def delete(self, node_external_id: str | T_TypeNode | T_TypeNodeList, traversal_count: int = 0):
        raise NotImplementedError()

    def list(self, traversal_count: int = 0, limit: int = 25) -> T_TypeNodeList:
        nodes = self._client.data_modeling.instances.list("node", sources=self.sources, limit=limit)

        return self.class_list([self.class_type.from_node(node) for node in nodes])
