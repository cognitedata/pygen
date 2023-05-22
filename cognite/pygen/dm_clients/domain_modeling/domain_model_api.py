from __future__ import annotations

import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Generic, Iterable, List, Optional, Set, Tuple, Type, TypeVar, get_args
from uuid import uuid4

from cognite.pygen.dm_clients.cdf.client_dm_v3 import EdgesAPI, NodesAPI
from cognite.pygen.dm_clients.cdf.data_classes_dm_v3 import Node, View

from .domain_client import DomainClient
from .domain_model import DomainModel
from .relationship_api import RelationshipAPI, RelationshipProxy

__all__ = [
    "DomainModelAPI",
]


_PendingEdgeT = Tuple[str, str, str]  # attr, start_ext_id, end_ext_id

logger = logging.getLogger(__name__)


DomainModelT = TypeVar("DomainModelT", bound=DomainModel)


class DomainModelAPI(Generic[DomainModelT]):
    """
    Node is an instance of a type in DM, i.e. an instance of a subclass of DomainModel,
    e.g: a single plant, a single pump...
    Caution: Naming is not terribly consistent inside cdf package!
    """

    def __init__(
        self,
        domain_model: Type[DomainModelT],
        view: View,
        nodes_api: NodesAPI,
        edges_api: EdgesAPI,
        domain_client: DomainClient,
        space_id: str,
        schema_version: int,
        api_version: Optional[str] = None,
    ):
        self.domain_model = domain_model
        self.view = view
        self.nodes_api = nodes_api
        self.relationships = RelationshipAPI(
            edges_api,
            domain_model,
            self,
            view,
            schema_version,
            space_id,
        )
        self.connect = RelationshipProxy(self)
        # TODO it would make more sense to pass RelationshipAPI directly instead of EdgesAPI
        self.domain_client = domain_client
        self.space_id = space_id
        self.schema_version = schema_version
        self.api_version = api_version

        self.model_external_id: str = f"{domain_model.__name__}_{schema_version}"

    def _prepare_items(self, items: Iterable[DomainModelT], ext_id_prefix: str = "") -> List[DomainModelT]:
        if not items:
            return []

        o2m_attrs = self.domain_model.get_one_to_many_attrs()

        for item in items:
            # invent externalId if missing:
            if not item.externalId:
                item.externalId = f"{type(item).__name__}_{uuid4().hex[:16]}"  # TODO configurable rnd hash length

            # DM doesn't like empty lists, making sure they are replaced with None:
            # TODO this ^ was true for v2, check again for v3
            for attr in o2m_attrs:
                val = getattr(item, attr)
                if isinstance(val, list) and not val:
                    setattr(item, attr, None)

        return list(items)

    def apply(self, items: Iterable[DomainModelT], ext_id_prefix: str = "") -> List[DomainModelT]:
        """
        Send provided nodes to the API.
        This is a multy-step job:
          0. Prepare the nodes - create random externalIDs if missing
          1. Create all nested nodes in new one-to-one relationships.
          2. Create all nested nodes in new one-to-many relationships.
          3. Create nodes.
          4. Create edges for those one-to-many relationships.
        """
        if ref_items := [item for item in items if item._reference]:
            raise ValueError(
                f"References passed into {type(self).__name__}.apply(): {[item.externalId for item in ref_items]}"
            )

        items = self._prepare_items(items, ext_id_prefix)

        if not items:
            return []

        with self.domain_client._cache_lock:
            self.domain_client.cache.delete_many(*[item.externalId for item in items if item.externalId])

        items = self._create_related_o2o_nodes(items)
        items, pending_edges = self._create_related_o2m_items(items)

        def _strip_items(item_data: dict) -> dict:
            o2m_attr = self.domain_model.get_one_to_many_attrs()
            return {key: val for key, val in item_data.items() if val is not None and key not in o2m_attr}

        def _make_ref(item_data: dict) -> dict:
            """Represents one-to-one relationships in CDF DM."""
            return {
                "space": self.space_id,  # TODO item.space_id ?
                "externalId": item_data["externalId"],
            }

        def _properties_data(data: dict) -> dict:
            """strip out reserved keys"""
            reserved_keys = {"externalId"}
            return {
                key: _make_ref(val) if key in self.domain_model.get_one_to_one_attrs() else val
                for key, val in data.items()
                if key not in reserved_keys
            }

        def _make_node(data: dict) -> Node:
            return Node(
                version=str(self.schema_version),
                space=self.space_id,
                externalId=data["externalId"],
                properties={
                    self.space_id: {
                        f"{self.view.externalId}/{self.view.version}": _properties_data(data),
                    },
                },
            )

        created_nodes = [_make_node(_strip_items(item.dict(by_alias=True, exclude_defaults=False))) for item in items]
        self.nodes_api.apply(self.view, nodes=created_nodes)

        self._create_related_o2m_edges(pending_edges)

        self._cache_created_items(items)
        return items

    def _create_related_o2o_nodes(self, items: List[DomainModelT]) -> List[DomainModelT]:
        """
        Create related nodes that are in a one-to-one relationship with nodes in `items`.
        Replace relevant attributes on individual `items` with references to the created nodes (API understands
        references and not nested objects).
        """
        o2o_edge_attrs = self.domain_model.get_one_to_one_attrs()
        for item in items:
            for attr, attr_value in list(item):
                if not attr_value or attr not in o2o_edge_attrs:
                    continue

                subitem_type = item.__fields__[attr].type_
                # TODO check:
                while type_args := get_args(subitem_type):
                    # type is wrapped in `Optional[]`
                    # TODO check that it's actually "Optional" and not something else!
                    subitem_type = type_args[0]

                if not attr_value._reference:
                    self.domain_client.apply([attr_value], ext_id_prefix=f"{item.externalId}__")
        return items

    def _create_related_o2m_items(
        self, items: List[DomainModelT]
    ) -> Tuple[List[DomainModelT], Dict[str, List[_PendingEdgeT]]]:
        """
        Create related nodes that are in a one-to-many relationship with the current item(s). Also prepare a list of
        edges that need to be created later (after the nodes in `items` are created too).
        Set the attribute value to `None`, so that the API isn't confused.
        """
        o2m_edge_attrs = self.domain_model.get_one_to_many_attrs()
        pending_edges: Dict[str, List[_PendingEdgeT]] = defaultdict(list)
        for item in items:
            if not item.externalId:
                raise ValueError("Unexpected empty externalId!")
            for attr, attr_value in list(item):
                if not attr_value or attr not in o2m_edge_attrs:
                    continue

                # We need to:
                #   1. create related nodes
                #   2. (after everything else is created) create Edge instances that point to those related nodes
                all_subitems = attr_value
                full_subitems = [subitem for subitem in all_subitems if not subitem._reference]
                self.domain_client.apply(full_subitems, ext_id_prefix=f"{item.externalId}__")
                for subitem in all_subitems:
                    pending_edges[attr].append((attr, item.externalId, subitem.externalId))
        return items, pending_edges

    def _create_related_o2m_edges(self, pending_edges: Dict[str, List[_PendingEdgeT]]):
        """
        All the nodes have been created at this point, now create the edges between them.
        """
        # TODO this is inefficient!
        for attr, pending_attr_edges in pending_edges.items():
            end_ext_ids_by_start = defaultdict(list)
            for pending_edge in pending_attr_edges:
                attr, start_ext_id, end_ext_id = pending_edge
                end_ext_ids_by_start[start_ext_id].append(end_ext_id)
            for start_ext_id, end_ext_ids in end_ext_ids_by_start.items():
                self.relationships.apply(
                    attribute=attr,
                    start_ext_id=start_ext_id,
                    end_ext_ids=[ext_id for ext_id in end_ext_ids],
                )

    def _cache_created_items(self, items: Iterable[DomainModelT]) -> None:
        """
        This caching is not just for performance! It also helps with CDF's "eventual consistency" promise.
        Without this cache, what happens is that we create a node (i.e. a DomainModel instance) and if we want to
        retrieve that same instance very soon, it often just isn't present in the response. After some time (seconds,
        sometimes minutes?) the new node appears in CDF.
        """
        for item in (item_ for item_ in items if item_.externalId and not item_._reference):
            # replace related items with reference dicts
            # ... one-to-many:
            o2m_subitems: Dict[str, Iterable[DomainModelT]] = {}
            for attr in item.get_one_to_many_attrs():
                o2m_subitems[attr] = getattr(item, attr, None) or []
                if o2m_subitems[attr]:
                    self._cache_created_items(o2m_subitems[attr])
                    setattr(
                        item,
                        attr,
                        [{"space": self.space_id, "externalId": subitem.externalId} for subitem in o2m_subitems[attr]],
                    )
            # ... one-to-one:
            o2o_subitems: Dict[str, Optional[DomainModelT]] = {}
            for attr in item.get_one_to_one_attrs():
                subitem = getattr(item, attr, None)
                o2o_subitems[attr] = subitem
                if subitem is not None:
                    self._cache_created_items([subitem])
                    setattr(item, attr, {"space": self.space_id, "externalId": subitem.externalId})
            # save to cache:
            if item.externalId:
                self.domain_client.cache.set(item.externalId, item)
            # restore replaced attributes:
            for attr in o2m_subitems:
                setattr(item, attr, o2m_subitems[attr])
            for attr in o2o_subitems:
                setattr(item, attr, o2o_subitems[attr])

    def _get_from_cache(self, external_ids: Iterable[str]) -> Tuple[List[DomainModelT], List[str]]:
        cached_items: List[DomainModelT] = []
        uncached_external_ids: Set[str] = set()
        for external_id in external_ids:
            # retrieve item from cache:
            cached_instance: DomainModelT = self.domain_client.cache.get(external_id)
            if cached_instance is not None:
                # retrieve related items from cache:
                # ... one-to-many:
                for attr in cached_instance.get_one_to_many_attrs():
                    subitem_ext_ids = [ref["externalId"] for ref in getattr(cached_instance, attr, None) or []]
                    cached_subs, uncached_sub_ids = self._get_from_cache(subitem_ext_ids)
                    if uncached_sub_ids:
                        uncached_external_ids.add(external_id)
                        break
                    setattr(cached_instance, attr, cached_subs)
                # ... one-to-one:
                for attr in cached_instance.get_one_to_one_attrs():
                    subitem_ext_id = (getattr(cached_instance, attr, None) or {}).get("externalId")
                    cached_subs, uncached_sub_ids = self._get_from_cache([subitem_ext_id] if subitem_ext_id else [])
                    if uncached_sub_ids:
                        uncached_external_ids.add(external_id)
                        break
                    setattr(cached_instance, attr, cached_subs[0] if cached_subs else None)
                # consider this item "cached" only if all its subitems are also cached:
                if cached_instance.externalId not in uncached_external_ids:
                    cached_items.append(cached_instance)
            else:
                uncached_external_ids.add(external_id)
        return cached_items, list(uncached_external_ids)

    def list(self, limit=25, resolve_relationships=True) -> List[DomainModelT]:
        nodes = self.nodes_api.list(self.view, limit=limit)
        if resolve_relationships:
            items = self._retrieve_full(nodes)
        else:
            items = self._retrieve_wo_rels(nodes)
        return items

    def retrieve(self, external_ids: Iterable[str]) -> List[DomainModelT]:
        cached_items, uncached_external_ids = self._get_from_cache(external_ids)
        retrieved_nodes = self.nodes_api.retrieve(self.view, uncached_external_ids)
        retrieved_instances = self._retrieve_full(retrieved_nodes)
        return [*cached_items, *retrieved_instances]  # TODO maintain order according to external_ids

    # TODO maybe implement delete_by_ext_ids? Note delete_related_items.

    def delete(self, items: Iterable[DomainModelT], delete_related_items: bool = False) -> None:
        if not items:
            return
        # Delete all edges that start on items.
        # Note: edges that _end_ on this node will remain unaffected!
        edges = self.relationships.list(from_ext_ids=[item.externalId for item in items if item.externalId])
        self.relationships.delete(edges)

        if delete_related_items:
            for attr in self.domain_model.get_one_to_one_attrs():
                self.domain_client.delete(
                    [subitem for item in items if (subitem := getattr(item, attr) is not None)],
                    delete_related_items=True,
                )
            for attr in self.domain_model.get_one_to_many_attrs():
                subitems: List[DomainModel] = []
                for item in items:
                    subitems.extend(subitem for subitem in getattr(item, attr, []) if subitem is not None)
                self.domain_client.delete(subitems, delete_related_items=True)

        external_ids = list({item.externalId for item in items if item.externalId})
        self.nodes_api.delete(self.space_id, external_ids)
        with self.domain_client._cache_lock:
            self.domain_client.cache.delete_many(*external_ids)

    def _retrieve_full(self, nodes: Iterable[Node]) -> List[DomainModelT]:
        """
        For every node, make a full DomainModel item, including all nested (related) objects.
        """
        full_items: Dict[str, DomainModelT]
        uncached_nodes: List[Node]

        # 1: retrieve nodes from cache

        cached_items, uncached_external_ids = self._get_from_cache([node.externalId for node in nodes])
        full_items = {item.externalId: item for item in cached_items if item.externalId}
        uncached_nodes = [node for node in nodes if node.externalId in uncached_external_ids]

        # 2: query the API for nodes that were not found in cache

        o2m_edge_attrs = self.domain_model.get_one_to_many_attrs()
        o2o_edge_attrs = self.domain_model.get_one_to_one_attrs()

        def _fetch_o2m_attr(attr_: str, related_domain_model_: Type[DomainModel], node_: Node) -> Any:
            edges = self.relationships.list([attr_], from_ext_ids=[node_.externalId])
            related_ext_ids = set(edge.endNode.externalId for edge in edges)
            related_items: List[DomainModel] = []
            uncached_ext_ids: List[str] = []
            if related_ext_ids:
                for related_ext_id in related_ext_ids:
                    cached_item_ = self.domain_client.cache.get(related_ext_id)
                    if cached_item_ is not None:
                        related_items.append(cached_item_)
                    else:
                        uncached_ext_ids.append(related_ext_id)
                domain_model_api_ = self.domain_client.get_api_for_domain_model(related_domain_model_)
                related_items.extend(domain_model_api_.retrieve(uncached_ext_ids))
            return related_items

        def _fetch_o2o_attr(attr_: str, related_domain_model_: Type[DomainModel], node_: Node) -> Any:
            attr_value = node_.get_properties(self.view).get(attr_)
            if attr_value is None:
                related_item = None
            else:
                domain_model_api = self.domain_client.get_api_for_domain_model(related_domain_model_)
                try:
                    related_item = (domain_model_api.retrieve([attr_value["externalId"]]) or [None])[0]
                except IndexError:
                    related_item = None
            return related_item

        # retrieve all related attributes in parallel:
        futures = []
        with ThreadPoolExecutor() as pool:
            for attr, related_domain_model in o2m_edge_attrs.items():
                for node in uncached_nodes:
                    futures.append((node, attr, pool.submit(_fetch_o2m_attr, attr, related_domain_model, node)))
            for attr, related_domain_model in o2o_edge_attrs.items():
                for node in uncached_nodes:
                    futures.append((node, attr, pool.submit(_fetch_o2o_attr, attr, related_domain_model, node)))
        for node, attr, future in futures:
            node.update_properties(self.view, {attr: future.result()})

        for node in uncached_nodes:
            item = self._make_item_from_node(node)
            if not item.externalId:
                raise ValueError("Unexpected empty externalId!")
            full_items[item.externalId] = item

        items = list(full_items.values())
        with self.domain_client._cache_lock:
            self._cache_created_items(items)
        return items

    def _retrieve_wo_rels(self, nodes: Iterable[Node]) -> List[DomainModelT]:
        """
        For every node make DomainModel item but set any relationship attributes to None.
        """
        # TODO Result should distinguish between unresolved relationships and legitimate `None` values?
        o2o_edge_attrs = self.domain_model.get_one_to_one_attrs()
        o2m_edge_attrs = self.domain_model.get_one_to_many_attrs()
        null_update = {attr: None for attr in [*o2o_edge_attrs, *o2m_edge_attrs]}
        return [self._make_item_from_node(node, null_update) for node in nodes]

    def _make_item_from_node(self, node: Node, properties_update: Optional[Dict[str, Any]] = None) -> DomainModelT:
        props = node.get_properties(self.view)
        if properties_update is not None:
            props.update(properties_update)
        return self.domain_model(externalId=node.externalId, **props)
