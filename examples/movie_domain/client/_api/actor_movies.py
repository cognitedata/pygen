from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from movie_domain.client.data_classes import Actor, ActorApply, ActorList, ActorApplyList, ActorFields, DomainModelApply
from movie_domain.client.data_classes._actor import _ACTOR_PROPERTIES_BY_FIELD


class ActorMoviesQuery:
    def __init__(
        self,
        client: CogniteClient,
        node_view: dm.ViewId,
        edge_view: dm.ViewId,
        end_node_view: dm.ViewId,
        node_limit: int = DEFAULT_LIMIT_READ,
        node_filter: dm.Filter | None = None,
    ):
        self._client = client
        self._node_view = node_view
        self._edge_view = edge_view
        self._end_node_view = end_node_view
        self._node_limit = node_limit
        self._node_filter = node_filter

    def list(
        self,
        space: str = "IntegrationTestsImmutable",
        limit: int | None = None,
        retrieve_movie: bool = True,
    ) -> ActorList:
        """List actors with movies.

        Args:
            space: The space where all the movies are located.
            limit: Maximum number of edges to return per unit procedure. Defaults to -1. Set to -1, float("inf") or None to return all items.
            retrieve_movie: Whether to retrieve `movie` for each movie. Defaults to True.

        Returns:
            List of actors with work units and optional movie.

        Examples:

            List 5 unit procedures with work units:

                    >>> from equipment_unit.client import EquipmentUnitClient
                    >>> client = EquipmentUnitClient()
                    >>> unit_procedures = client.unit_procedure.work_units(limit=5).list()

        """
        f = dm.filters
        edge_filter = _create_filter_work_units(
            self._edge_view,
            None,
            None,
            space,
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "UnitProcedure.equipment_module"},
            ),
        )
        limit = float("inf") if limit is None or limit == -1 else limit
        cursors = {"nodes": None, "edges": None}
        results = {"nodes": [], "edges": []}
        total_retrieved = {"nodes": 0, "edges": 0}
        limits = {"nodes": self._node_limit, "edges": limit}
        if retrieve_movie:
            cursors["end_nodes"] = None
            results["end_nodes"] = []
            total_retrieved["end_nodes"] = 0
            limits["end_nodes"] = float("nan")

        while True:
            query_limits = {k: min(INSTANCE_QUERY_LIMIT, v - total_retrieved[k]) for k, v in limits.items()}

            selected_nodes = dm.query.NodeResultSetExpression(filter=self._node_filter, limit=query_limits["nodes"])
            selected_edges = dm.query.EdgeResultSetExpression(
                from_="nodes", filter=edge_filter, limit=query_limits["edges"]
            )
            with_ = {
                "nodes": selected_nodes,
                "edges": selected_edges,
            }
            if retrieve_movie:
                with_["end_nodes"] = dm.query.NodeResultSetExpression(from_="edges", limit=query_limits["end_nodes"])

            select = {
                "nodes": dm.query.Select(
                    [dm.query.SourceSelector(self._node_view, list(_UNITPROCEDURE_PROPERTIES_BY_FIELD.values()))],
                ),
                "edges": dm.query.Select(
                    [dm.query.SourceSelector(self._edge_view, list(_STARTENDTIME_PROPERTIES_BY_FIELD.values()))],
                ),
            }
            if retrieve_movie:
                select["end_nodes"] = dm.query.Select(
                    [dm.query.SourceSelector(self._end_node_view, list(_EQUIPMENTMODULE_PROPERTIES_BY_FIELD.values()))]
                )

            query = dm.query.Query(with_=with_, select=select, cursors=cursors)
            batch = self._client.data_modeling.instances.query(query)
            for key in total_retrieved:
                total_retrieved[key] += len(batch[key])
                results[key].extend(batch[key])
                cursors[key] = batch.cursors[key]

            if all(
                total_retrieved[k] >= limits[k] or cursors[k] is None or len(batch[k]) == 0 for k in total_retrieved
            ):
                break

        if retrieve_equipment_module:
            end_node_by_id = {
                (node.space, node.external_id): EquipmentModule.from_node(node) for node in results["end_nodes"]
            }
        else:
            end_node_by_id = {}

        edge_by_start_node = defaultdict(list)
        for edge in results["edges"]:
            edge = StartEndTime.from_edge(edge)
            edge.equipment_module = end_node_by_id.get((edge.end_node.space, edge.end_node.external_id))
            edge_by_start_node[(edge.start_node.space, edge.start_node.external_id)].append(edge)

        nodes = []
        for node in results["nodes"]:
            node = UnitProcedure.from_node(node)
            node.work_units = edge_by_start_node.get((node.space, node.external_id), [])
            nodes.append(node)

        return UnitProcedureList(nodes)


class ActorMoviesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def list(
        self,
        actor_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List movies edges of a actor.

        Args:
            actor_id: ID of the source actor.
            limit: Maximum number of movie edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the movie edges are located.

        Returns:
            The requested movie edges.

        Examples:

            List 5 movies edges connected to "my_actor":

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> actor = client.actor.movies.list("my_actor", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "Role.movies"},
            )
        ]
        if actor_id:
            actor_ids = actor_id if isinstance(actor_id, list) else [actor_id]
            is_actors = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in actor_ids
                ],
            )
            filters.append(is_actors)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))
