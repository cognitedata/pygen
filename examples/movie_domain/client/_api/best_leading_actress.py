from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from movie_domain.client.data_classes import (
    BestLeadingActress,
    BestLeadingActressApply,
    BestLeadingActressList,
    BestLeadingActressApplyList,
    BestLeadingActressFields,
    BestLeadingActressTextFields,
    DomainModelApply,
)
from movie_domain.client.data_classes._best_leading_actress import _BESTLEADINGACTRESS_PROPERTIES_BY_FIELD


class BestLeadingActressAPI(TypeAPI[BestLeadingActress, BestLeadingActressApply, BestLeadingActressList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[BestLeadingActressApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BestLeadingActress,
            class_apply_type=BestLeadingActressApply,
            class_list=BestLeadingActressList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, best_leading_actress: BestLeadingActressApply | Sequence[BestLeadingActressApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) best leading actresses.

        Args:
            best_leading_actress: Best leading actress or sequence of best leading actresses to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new best_leading_actress:

                >>> from movie_domain.client import MovieClient
                >>> from movie_domain.client.data_classes import BestLeadingActressApply
                >>> client = MovieClient()
                >>> best_leading_actress = BestLeadingActressApply(external_id="my_best_leading_actress", ...)
                >>> result = client.best_leading_actress.apply(best_leading_actress)

        """
        if isinstance(best_leading_actress, BestLeadingActressApply):
            instances = best_leading_actress.to_instances_apply(self._view_by_write_class)
        else:
            instances = BestLeadingActressApplyList(best_leading_actress).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more best leading actress.

        Args:
            external_id: External id of the best leading actress to delete.
            space: The space where all the best leading actress are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete best_leading_actress by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> client.best_leading_actress.delete("my_best_leading_actress")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> BestLeadingActress:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BestLeadingActressList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> BestLeadingActress | BestLeadingActressList:
        """Retrieve one or more best leading actresses by id(s).

        Args:
            external_id: External id or list of external ids of the best leading actresses.
            space: The space where all the best leading actresses are located.

        Returns:
            The requested best leading actresses.

        Examples:

            Retrieve best_leading_actress by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> best_leading_actress = client.best_leading_actress.retrieve("my_best_leading_actress")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: BestLeadingActressTextFields | Sequence[BestLeadingActressTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BestLeadingActressList:
        """Search best leading actresses

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best leading actresses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results best leading actresses matching the query.

        Examples:

           Search for 'my_best_leading_actress' in all text properties:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> best_leading_actresses = client.best_leading_actress.search('my_best_leading_actress')

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _BESTLEADINGACTRESS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BestLeadingActressFields | Sequence[BestLeadingActressFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BestLeadingActressTextFields | Sequence[BestLeadingActressTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
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
        property: BestLeadingActressFields | Sequence[BestLeadingActressFields] | None = None,
        group_by: BestLeadingActressFields | Sequence[BestLeadingActressFields] = None,
        query: str | None = None,
        search_properties: BestLeadingActressTextFields | Sequence[BestLeadingActressTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
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
        property: BestLeadingActressFields | Sequence[BestLeadingActressFields] | None = None,
        group_by: BestLeadingActressFields | Sequence[BestLeadingActressFields] | None = None,
        query: str | None = None,
        search_property: BestLeadingActressTextFields | Sequence[BestLeadingActressTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across best leading actresses

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best leading actresses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count best leading actresses in space `my_space`:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.best_leading_actress.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BESTLEADINGACTRESS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BestLeadingActressFields,
        interval: float,
        query: str | None = None,
        search_property: BestLeadingActressTextFields | Sequence[BestLeadingActressTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for best leading actresses

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best leading actresses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BESTLEADINGACTRESS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BestLeadingActressList:
        """List/filter best leading actresses

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best leading actresses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested best leading actresses

        Examples:

            List best leading actresses and limit to 5:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> best_leading_actresses = client.best_leading_actress.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_year: int | None = None,
    max_year: int | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if min_year or max_year:
        filters.append(dm.filters.Range(view_id.as_property_ref("year"), gte=min_year, lte=max_year))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
