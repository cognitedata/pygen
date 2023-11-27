from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from movie_domain.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Actor,
    ActorApply,
    ActorFields,
    ActorList,
    ActorApplyList,
)
from movie_domain.client.data_classes._actor import (
    _ACTOR_PROPERTIES_BY_FIELD,
    _create_actor_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .actor_movies import ActorMoviesAPI
from .actor_nomination import ActorNominationAPI
from .actor_query import ActorQueryAPI


class ActorAPI(NodeAPI[Actor, ActorApply, ActorList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[ActorApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Actor,
            class_apply_type=ActorApply,
            class_list=ActorList,
            class_apply_list=ActorApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.movies_edge = ActorMoviesAPI(client)
        self.nomination_edge = ActorNominationAPI(client)

    def __call__(
        self,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ActorQueryAPI[ActorList]:
        """Query starting at actors.

        Args:
            person: The person to filter on.
            won_oscar: The won oscar to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of actors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for actors.

        """
        filter_ = _create_actor_filter(
            self._view_id,
            person,
            won_oscar,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            ActorList,
            [
                QueryStep(
                    name="actor",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_ACTOR_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Actor,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return ActorQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, actor: ActorApply | Sequence[ActorApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) actors.

        Note: This method iterates through all nodes and timeseries linked to actor and creates them including the edges
        between the nodes. For example, if any of `movies` or `nomination` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            actor: Actor or sequence of actors to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new actor:

                >>> from movie_domain.client import MovieClient
                >>> from movie_domain.client.data_classes import ActorApply
                >>> client = MovieClient()
                >>> actor = ActorApply(external_id="my_actor", ...)
                >>> result = client.actor.apply(actor)

        """
        return self._apply(actor, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
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
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Actor | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> ActorList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> Actor | ActorList | None:
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
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_triple=[
                (self.movies_edge, "movies", dm.DirectRelationReference("IntegrationTestsImmutable", "Role.movies")),
                (
                    self.nomination_edge,
                    "nomination",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "Role.nomination"),
                ),
            ],
        )

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

        Returns:
            Aggregation results.

        Examples:

            Count actors in space `my_space`:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.actor.aggregate("count", space="my_space")

        """

        filter_ = _create_actor_filter(
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

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_actor_filter(
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
        filter_ = _create_actor_filter(
            self._view_id,
            person,
            won_oscar,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_triple=[
                (self.movies_edge, "movies", dm.DirectRelationReference("IntegrationTestsImmutable", "Role.movies")),
                (
                    self.nomination_edge,
                    "nomination",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "Role.nomination"),
                ),
            ],
        )
