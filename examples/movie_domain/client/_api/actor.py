from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from movie_domain.client.data_classes import Actor, ActorApply, ActorList, ActorApplyList, ActorFields, DomainModelApply
from movie_domain.client.data_classes._actor import _ACTOR_PROPERTIES_BY_FIELD


class ActorMoviesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        """Retrieve one or more movies edges by id(s) of a actor.

        Args:
            external_id: External id or list of external ids source actor.
            space: The space where all the movie edges are located.

        Returns:
            The requested movie edges.

        Examples:

            Retrieve movies edge by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> actor = client.actor.movies.retrieve("my_movies")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Role.movies"},
        )
        if isinstance(external_id, str):
            is_actor = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_actor))

        else:
            is_actors = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_actors))

    def list(
        self, actor_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """List movies edges of a actor.

        Args:
            actor_id: Id of the source actor.
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
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Role.movies"},
        )
        filters.append(is_edge_type)
        if actor_id:
            actor_ids = [actor_id] if isinstance(actor_id, str) else actor_id
            is_actors = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in actor_ids],
            )
            filters.append(is_actors)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ActorNominationAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        """Retrieve one or more nomination edges by id(s) of a actor.

        Args:
            external_id: External id or list of external ids source actor.
            space: The space where all the nomination edges are located.

        Returns:
            The requested nomination edges.

        Examples:

            Retrieve nomination edge by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> actor = client.actor.nomination.retrieve("my_nomination")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Role.nomination"},
        )
        if isinstance(external_id, str):
            is_actor = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_actor))

        else:
            is_actors = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_actors))

    def list(
        self, actor_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """List nomination edges of a actor.

        Args:
            actor_id: Id of the source actor.
            limit: Maximum number of nomination edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the nomination edges are located.

        Returns:
            The requested nomination edges.

        Examples:

            List 5 nomination edges connected to "my_actor":

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> actor = client.actor.nomination.list("my_actor", limit=5)

        """
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Role.nomination"},
        )
        filters.append(is_edge_type)
        if actor_id:
            actor_ids = [actor_id] if isinstance(actor_id, str) else actor_id
            is_actors = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in actor_ids],
            )
            filters.append(is_actors)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ActorAPI(TypeAPI[Actor, ActorApply, ActorList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[ActorApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Actor,
            class_apply_type=ActorApply,
            class_list=ActorList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.movies = ActorMoviesAPI(client)
        self.nomination = ActorNominationAPI(client)

    def apply(self, actor: ActorApply | Sequence[ActorApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) actors.

        Note: This method iterates through all nodes linked to actor and create them including the edges
        between the nodes. For example, if any of `movies` or `nomination` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            actor: Actor or sequence of actors to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new actor:

                >>> from movie_domain.client import MovieClient
                >>> from movie_domain.client.data_classes import ActorApply
                >>> client = MovieClient()
                >>> actor = ActorApply(external_id="my_actor", ...)
                >>> result = client.actor.apply(actor)

        """
        if isinstance(actor, ActorApply):
            instances = actor.to_instances_apply(self._view_by_write_class)
        else:
            instances = ActorApplyList(actor).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more actor.

        Args:
            external_id: External id of the actor to delete.
            space: The space where all the actor are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete actor by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> client.actor.delete("my_actor")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Actor:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ActorList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable") -> Actor | ActorList:
        """Retrieve one or more actors by id(s).

        Args:
            external_id: External id or list of external ids of the actors.
            space: The space where all the actors are located.

        Returns:
            The requested actors.

        Examples:

            Retrieve actor by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> actor = client.actor.retrieve("my_actor")

        """
        if isinstance(external_id, str):
            actor = self._retrieve((space, external_id))

            movie_edges = self.movies.retrieve(external_id)
            actor.movies = [edge.end_node.external_id for edge in movie_edges]
            nomination_edges = self.nomination.retrieve(external_id)
            actor.nomination = [edge.end_node.external_id for edge in nomination_edges]

            return actor
        else:
            actors = self._retrieve([(space, ext_id) for ext_id in external_id])

            movie_edges = self.movies.retrieve(external_id)
            self._set_movies(actors, movie_edges)
            nomination_edges = self.nomination.retrieve(external_id)
            self._set_nomination(actors, nomination_edges)

            return actors

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ActorFields | Sequence[ActorFields] | None = None,
        group_by: None = None,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ActorFields | Sequence[ActorFields] | None = None,
        group_by: ActorFields | Sequence[ActorFields] = None,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ActorFields | Sequence[ActorFields] | None = None,
        group_by: ActorFields | Sequence[ActorFields] | None = None,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across actors

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            person: The person to filter on.
            won_oscar: The won oscar to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of actors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `movies` or `nomination` external ids for the actors. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count actors in space `my_space`:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.actor.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            person,
            won_oscar,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ACTOR_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ActorFields,
        interval: float,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for actors

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            person: The person to filter on.
            won_oscar: The won oscar to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of actors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `movies` or `nomination` external ids for the actors. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            person,
            won_oscar,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ACTOR_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ActorList:
        """List/filter actors

        Args:
            person: The person to filter on.
            won_oscar: The won oscar to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of actors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `movies` or `nomination` external ids for the actors. Defaults to True.

        Returns:
            List of requested actors

        Examples:

            List actors and limit to 5:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> actors = client.actor.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            person,
            won_oscar,
            external_id_prefix,
            space,
            filter,
        )

        actors = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := actors.as_external_ids()) > IN_FILTER_LIMIT:
                movie_edges = self.movies.list(limit=-1)
            else:
                movie_edges = self.movies.list(external_ids, limit=-1)
            self._set_movies(actors, movie_edges)
            if len(external_ids := actors.as_external_ids()) > IN_FILTER_LIMIT:
                nomination_edges = self.nomination.list(limit=-1)
            else:
                nomination_edges = self.nomination.list(external_ids, limit=-1)
            self._set_nomination(actors, nomination_edges)

        return actors

    @staticmethod
    def _set_movies(actors: Sequence[Actor], movie_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in movie_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for actor in actors:
            node_id = actor.id_tuple()
            if node_id in edges_by_start_node:
                actor.movies = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_nomination(actors: Sequence[Actor], nomination_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in nomination_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for actor in actors:
            node_id = actor.id_tuple()
            if node_id in edges_by_start_node:
                actor.nomination = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    won_oscar: bool | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if person and isinstance(person, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("person"), value={"space": "IntegrationTestsImmutable", "externalId": person}
            )
        )
    if person and isinstance(person, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("person"), value={"space": person[0], "externalId": person[1]})
        )
    if person and isinstance(person, list) and isinstance(person[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("person"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in person],
            )
        )
    if person and isinstance(person, list) and isinstance(person[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("person"), values=[{"space": item[0], "externalId": item[1]} for item in person]
            )
        )
    if won_oscar and isinstance(won_oscar, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("wonOscar"), value=won_oscar))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
