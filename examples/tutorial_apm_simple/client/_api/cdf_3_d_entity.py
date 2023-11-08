from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from tutorial_apm_simple.client.data_classes import CdfEntity, CdfEntityApply, CdfEntityList, CdfEntityApplyList


class CdfEntityInModelAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space: str = "cdf_3d_schema") -> dm.EdgeList:
        """Retrieve one or more in_model_3_d edges by id(s) of a cdf 3 d entity.

        Args:
            external_id: External id or list of external ids source cdf 3 d entity.
            space: The space where all the in model 3 d edges are located.

        Returns:
            The requested in model 3 d edges.

        Examples:

            Retrieve in_model_3_d edge by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_entity = client.cdf_3_d_entity.in_model_3_d.retrieve("my_in_model_3_d")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cdf_3d_schema", "externalId": "cdf3dEntityConnection"},
        )
        if isinstance(external_id, str):
            is_cdf_3_d_entities = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
        else:
            is_cdf_3_d_entities = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_cdf_3_d_entities)
        )

    def list(
        self,
        cdf_3_d_entity_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "cdf_3d_schema",
    ) -> dm.EdgeList:
        """List in_model_3_d edges of a cdf 3 d entity.

        Args:
            cdf_3_d_entity_id: ID of the source cdf 3 d entity.
            limit: Maximum number of in model 3 d edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the in model 3 d edges are located.

        Returns:
            The requested in model 3 d edges.

        Examples:

            List 5 in_model_3_d edges connected to "my_cdf_3_d_entity":

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_entity = client.cdf_3_d_entity.in_model_3_d.list("my_cdf_3_d_entity", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "cdf_3d_schema", "externalId": "cdf3dEntityConnection"},
            )
        ]
        if cdf_3_d_entity_id:
            cdf_3_d_entity_ids = cdf_3_d_entity_id if isinstance(cdf_3_d_entity_id, list) else [cdf_3_d_entity_id]
            is_cdf_3_d_entities = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in cdf_3_d_entity_ids
                ],
            )
            filters.append(is_cdf_3_d_entities)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class CdfEntityAPI(TypeAPI[CdfEntity, CdfEntityApply, CdfEntityList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CdfEntityApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CdfEntity,
            class_apply_type=CdfEntityApply,
            class_list=CdfEntityList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.in_model_3_d = CdfEntityInModelAPI(client)

    def apply(
        self, cdf_3_d_entity: CdfEntityApply | Sequence[CdfEntityApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) cdf 3 d entities.

        Note: This method iterates through all nodes linked to cdf_3_d_entity and create them including the edges
        between the nodes. For example, if any of `in_model_3_d` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            cdf_3_d_entity: Cdf 3 d entity or sequence of cdf 3 d entities to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new cdf_3_d_entity:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> from tutorial_apm_simple.client.data_classes import CdfEntityApply
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_entity = CdfEntityApply(external_id="my_cdf_3_d_entity", ...)
                >>> result = client.cdf_3_d_entity.apply(cdf_3_d_entity)

        """
        if isinstance(cdf_3_d_entity, CdfEntityApply):
            instances = cdf_3_d_entity.to_instances_apply(self._view_by_write_class)
        else:
            instances = CdfEntityApplyList(cdf_3_d_entity).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space: str = "cdf_3d_schema") -> dm.InstancesDeleteResult:
        """Delete one or more cdf 3 d entity.

        Args:
            external_id: External id of the cdf 3 d entity to delete.
            space: The space where all the cdf 3 d entity are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cdf_3_d_entity by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> client.cdf_3_d_entity.delete("my_cdf_3_d_entity")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CdfEntity:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CdfEntityList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "cdf_3d_schema") -> CdfEntity | CdfEntityList:
        """Retrieve one or more cdf 3 d entities by id(s).

        Args:
            external_id: External id or list of external ids of the cdf 3 d entities.
            space: The space where all the cdf 3 d entities are located.

        Returns:
            The requested cdf 3 d entities.

        Examples:

            Retrieve cdf_3_d_entity by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_entity = client.cdf_3_d_entity.retrieve("my_cdf_3_d_entity")

        """
        if isinstance(external_id, str):
            cdf_3_d_entity = self._retrieve((space, external_id))

            in_model_3_d_edges = self.in_model_3_d.retrieve(external_id, space=space)
            cdf_3_d_entity.in_model_3_d = [edge.end_node.external_id for edge in in_model_3_d_edges]

            return cdf_3_d_entity
        else:
            cdf_3_d_entities = self._retrieve([(space, ext_id) for ext_id in external_id])

            in_model_3_d_edges = self.in_model_3_d.retrieve(cdf_3_d_entities.as_node_ids())
            self._set_in_model_3_d(cdf_3_d_entities, in_model_3_d_edges)

            return cdf_3_d_entities

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> CdfEntityList:
        """List/filter cdf 3 d entities

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d entities to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `in_model_3_d` external ids for the cdf 3 d entities. Defaults to True.

        Returns:
            List of requested cdf 3 d entities

        Examples:

            List cdf 3 d entities and limit to 5:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_entities = client.cdf_3_d_entity.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )

        cdf_3_d_entities = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            space_arg = {"space": space} if space else {}
            if len(ids := cdf_3_d_entities.as_node_ids()) > IN_FILTER_LIMIT:
                in_model_3_d_edges = self.in_model_3_d.list(limit=-1, **space_arg)
            else:
                in_model_3_d_edges = self.in_model_3_d.list(ids, limit=-1)
            self._set_in_model_3_d(cdf_3_d_entities, in_model_3_d_edges)

        return cdf_3_d_entities

    @staticmethod
    def _set_in_model_3_d(cdf_3_d_entities: Sequence[CdfEntity], in_model_3_d_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in in_model_3_d_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for cdf_3_d_entity in cdf_3_d_entities:
            node_id = cdf_3_d_entity.id_tuple()
            if node_id in edges_by_start_node:
                cdf_3_d_entity.in_model_3_d = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
