from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List, Sequence, Type

from cognite.dm_clients.cdf.client_dm_v3 import EdgesAPI
from cognite.dm_clients.cdf.data_classes_dm_v3 import Edge, RelationReference, View

from .domain_model import DomainModel

logger = logging.getLogger(__name__)


class RelationshipAPI:
    """
    This is a wrapper for EdgesAPI which simplifies its use, specifically:
     * constructs `edge.type` values (from model_type, and attribute),
     * works with a single space_id,
     * simplifies creation of Edge instances (does not require them to be created outside).

    This class is considered "internal". DomainModelAPI uses it, so it should not be necessary to use it directly.
    """

    def __init__(
        self,
        edges_api: EdgesAPI,
        model_type: Type[DomainModel],
        view: View,
        schema_version: int,
        space_id: str,
    ):
        self.edges_api = edges_api
        self.model_type = model_type
        self.view = view
        self.schema_version = schema_version
        self.space_id = space_id

    def create(self, attribute: str, start_ext_id: str, end_ext_ids: Iterable[str]) -> None:
        """
        Crete an Edge on a particular attribute of the `self.model_type` type of instance.
        Additionally:
         * delete obsolete edges
         * don't create duplicate edges (if some exist from before)
        """
        edge_type_ext_id = f"{self.model_type.__name__}.{attribute}"
        edges = [
            Edge(
                externalId=f"{start_ext_id}.{attribute}__{end_ext_id}",
                space=self.space_id,
                version=str(self.schema_version),
                type=RelationReference(space=self.space_id, externalId=edge_type_ext_id),
                startNode=RelationReference(space=self.space_id, externalId=start_ext_id),
                endNode=RelationReference(space=self.space_id, externalId=end_ext_id),
            )
            for end_ext_id in end_ext_ids
        ]

        existing_edges = self.edges_api.list(self.view, [attribute], [start_ext_id])
        existing_end_ext_ids = [edge.endNode.externalId for edge in existing_edges]
        edges_to_delete = [edge for edge in existing_edges if edge.endNode.externalId not in end_ext_ids]
        edges_to_create = [edge for edge in edges if edge.endNode.externalId not in existing_end_ext_ids]

        with ThreadPoolExecutor(max_workers=2) as pool:
            pool.submit(self.delete, edges_to_delete)
            pool.submit(self.edges_api.create, edges_to_create)

    def list(self, attributes: Sequence[str] = (), from_ext_ids: Sequence[str] = (), limit=1000) -> List[Edge]:
        """
        List all the edges for an attribute or all attributes if empty.
        Note about `limit`: it applies to each attribute individually, so if you request multiple attributes (or all of
        them, which is the default) you can expect to get more than `limit` of items back, but at most `limit` items
        for each attribute.
        """
        if len(attributes) == 0:
            attributes = list(self.model_type.get_one_to_many_attrs())
        edges: List[Edge] = []
        retrieved_edges = self.edges_api.list(
            self.view,
            attributes,
            from_ext_ids,
            limit=limit,
        )
        edges.extend(retrieved_edges)
        return edges

    def delete(self, items: Iterable[Edge]) -> None:
        self.edges_api.delete(self.space_id, list({edge.externalId for edge in items}))
