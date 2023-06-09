from __future__ import annotations

from typing import Generic, Sequence, Type, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from movie_domain.client.data_classes._core import T_TypeApplyNode, T_TypeNode, T_TypeNodeList


class TypeAPI(Generic[T_TypeNode, T_TypeApplyNode, T_TypeNodeList]):
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

    @overload
    def _retrieve(self, external_id: str) -> T_TypeNode:
        ...

    @overload
    def _retrieve(self, external_id: Sequence[str]) -> T_TypeNodeList:
        ...

    def _retrieve(
        self, nodes: dm.NodeId | Sequence[dm.NodeId] | tuple[str, str] | Sequence[tuple[str, str]]
    ) -> T_TypeNode | T_TypeNodeList:
        is_multiple = (
            isinstance(nodes, Sequence)
            and not isinstance(nodes, str)
            and not (isinstance(nodes, tuple) and isinstance(nodes[0], str))
        )
        instances = self._client.data_modeling.instances.retrieve(nodes=nodes, sources=self.sources)
        if is_multiple:
            return self.class_list([self.class_type.from_node(node) for node in instances.nodes])
        return self.class_type.from_node(instances.nodes[0])

    def _list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> T_TypeNodeList:
        nodes = self._client.data_modeling.instances.list("node", sources=self.sources, limit=limit)
        return self.class_list([self.class_type.from_node(node) for node in nodes])
