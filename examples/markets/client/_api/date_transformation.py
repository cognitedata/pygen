from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from markets.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    DateTransformation,
    DateTransformationApply,
    DateTransformationFields,
    DateTransformationList,
    DateTransformationTextFields,
)
from markets.client.data_classes._date_transformation import (
    _DATETRANSFORMATION_PROPERTIES_BY_FIELD,
    _create_date_transformation_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .date_transformation_query import DateTransformationQueryAPI


class DateTransformationAPI(NodeAPI[DateTransformation, DateTransformationApply, DateTransformationList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[DateTransformationApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DateTransformation,
            class_apply_type=DateTransformationApply,
            class_list=DateTransformationList,
            class_apply_list=DateTransformationApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DateTransformationQueryAPI[DateTransformationList]:
        """Query starting at date transformations.

        Args:
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of date transformations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for date transformations.

        """
        filter_ = _create_date_transformation_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            DateTransformationList,
            [
                QueryStep(
                    name="date_transformation",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_DATETRANSFORMATION_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=DateTransformation,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return DateTransformationQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, date_transformation: DateTransformationApply | Sequence[DateTransformationApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) date transformations.

        Args:
            date_transformation: Date transformation or sequence of date transformations to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new date_transformation:

                >>> from markets.client import MarketClient
                >>> from markets.client.data_classes import DateTransformationApply
                >>> client = MarketClient()
                >>> date_transformation = DateTransformationApply(external_id="my_date_transformation", ...)
                >>> result = client.date_transformation.apply(date_transformation)

        """
        return self._apply(date_transformation, replace)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = "market") -> dm.InstancesDeleteResult:
        """Delete one or more date transformation.

        Args:
            external_id: External id of the date transformation to delete.
            space: The space where all the date transformation are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete date_transformation by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> client.date_transformation.delete("my_date_transformation")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> DateTransformation:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> DateTransformationList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "market"
    ) -> DateTransformation | DateTransformationList:
        """Retrieve one or more date transformations by id(s).

        Args:
            external_id: External id or list of external ids of the date transformations.
            space: The space where all the date transformations are located.

        Returns:
            The requested date transformations.

        Examples:

            Retrieve date_transformation by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> date_transformation = client.date_transformation.retrieve("my_date_transformation")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: DateTransformationTextFields | Sequence[DateTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DateTransformationList:
        """Search date transformations

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of date transformations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results date transformations matching the query.

        Examples:

           Search for 'my_date_transformation' in all text properties:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> date_transformations = client.date_transformation.search('my_date_transformation')

        """
        filter_ = _create_date_transformation_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _DATETRANSFORMATION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DateTransformationFields | Sequence[DateTransformationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: DateTransformationTextFields | Sequence[DateTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
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
        property: DateTransformationFields | Sequence[DateTransformationFields] | None = None,
        group_by: DateTransformationFields | Sequence[DateTransformationFields] = None,
        query: str | None = None,
        search_properties: DateTransformationTextFields | Sequence[DateTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
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
        property: DateTransformationFields | Sequence[DateTransformationFields] | None = None,
        group_by: DateTransformationFields | Sequence[DateTransformationFields] | None = None,
        query: str | None = None,
        search_property: DateTransformationTextFields | Sequence[DateTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across date transformations

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of date transformations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count date transformations in space `my_space`:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.date_transformation.aggregate("count", space="my_space")

        """

        filter_ = _create_date_transformation_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _DATETRANSFORMATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: DateTransformationFields,
        interval: float,
        query: str | None = None,
        search_property: DateTransformationTextFields | Sequence[DateTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for date transformations

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of date transformations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_date_transformation_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _DATETRANSFORMATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DateTransformationList:
        """List/filter date transformations

        Args:
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of date transformations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested date transformations

        Examples:

            List date transformations and limit to 5:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> date_transformations = client.date_transformation.list(limit=5)

        """
        filter_ = _create_date_transformation_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
