from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    LineageAssertions,
    LineageAssertionsApply,
    LineageAssertionsFields,
    LineageAssertionsList,
    LineageAssertionsApplyList,
    LineageAssertionsTextFields,
)
from osdu_wells.client.data_classes._lineage_assertions import (
    _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD,
    _create_lineage_assertion_filter,
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
from .lineage_assertions_query import LineageAssertionsQueryAPI


class LineageAssertionsAPI(NodeAPI[LineageAssertions, LineageAssertionsApply, LineageAssertionsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[LineageAssertionsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=LineageAssertions,
            class_apply_type=LineageAssertionsApply,
            class_list=LineageAssertionsList,
            class_apply_list=LineageAssertionsApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> LineageAssertionsQueryAPI[LineageAssertionsList]:
        """Query starting at lineage assertions.

        Args:
            id_: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            lineage_relationship_type: The lineage relationship type to filter on.
            lineage_relationship_type_prefix: The prefix of the lineage relationship type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of lineage assertions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for lineage assertions.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_lineage_assertion_filter(
            self._view_id,
            id_,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(LineageAssertionsList)
        return LineageAssertionsQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, lineage_assertion: LineageAssertionsApply | Sequence[LineageAssertionsApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) lineage assertions.

        Args:
            lineage_assertion: Lineage assertion or sequence of lineage assertions to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new lineage_assertion:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import LineageAssertionsApply
                >>> client = OSDUClient()
                >>> lineage_assertion = LineageAssertionsApply(external_id="my_lineage_assertion", ...)
                >>> result = client.lineage_assertions.apply(lineage_assertion)

        """
        return self._apply(lineage_assertion, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more lineage assertion.

        Args:
            external_id: External id of the lineage assertion to delete.
            space: The space where all the lineage assertion are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete lineage_assertion by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.lineage_assertions.delete("my_lineage_assertion")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> LineageAssertions | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> LineageAssertionsList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> LineageAssertions | LineageAssertionsList | None:
        """Retrieve one or more lineage assertions by id(s).

        Args:
            external_id: External id or list of external ids of the lineage assertions.
            space: The space where all the lineage assertions are located.

        Returns:
            The requested lineage assertions.

        Examples:

            Retrieve lineage_assertion by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> lineage_assertion = client.lineage_assertions.retrieve("my_lineage_assertion")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LineageAssertionsList:
        """Search lineage assertions

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            id_: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            lineage_relationship_type: The lineage relationship type to filter on.
            lineage_relationship_type_prefix: The prefix of the lineage relationship type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of lineage assertions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results lineage assertions matching the query.

        Examples:

           Search for 'my_lineage_assertion' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> lineage_assertions = client.lineage_assertions.search('my_lineage_assertion')

        """
        filter_ = _create_lineage_assertion_filter(
            self._view_id,
            id_,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
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
        property: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        group_by: LineageAssertionsFields | Sequence[LineageAssertionsFields] = None,
        query: str | None = None,
        search_properties: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
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
        property: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        group_by: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        query: str | None = None,
        search_property: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across lineage assertions

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            id_: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            lineage_relationship_type: The lineage relationship type to filter on.
            lineage_relationship_type_prefix: The prefix of the lineage relationship type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of lineage assertions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count lineage assertions in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.lineage_assertions.aggregate("count", space="my_space")

        """

        filter_ = _create_lineage_assertion_filter(
            self._view_id,
            id_,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: LineageAssertionsFields,
        interval: float,
        query: str | None = None,
        search_property: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for lineage assertions

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            id_: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            lineage_relationship_type: The lineage relationship type to filter on.
            lineage_relationship_type_prefix: The prefix of the lineage relationship type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of lineage assertions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_lineage_assertion_filter(
            self._view_id,
            id_,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LineageAssertionsList:
        """List/filter lineage assertions

        Args:
            id_: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            lineage_relationship_type: The lineage relationship type to filter on.
            lineage_relationship_type_prefix: The prefix of the lineage relationship type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of lineage assertions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested lineage assertions

        Examples:

            List lineage assertions and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> lineage_assertions = client.lineage_assertions.list(limit=5)

        """
        filter_ = _create_lineage_assertion_filter(
            self._view_id,
            id_,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
