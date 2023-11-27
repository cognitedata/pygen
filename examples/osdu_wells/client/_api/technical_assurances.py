from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    TechnicalAssurances,
    TechnicalAssurancesApply,
    TechnicalAssurancesFields,
    TechnicalAssurancesList,
    TechnicalAssurancesApplyList,
    TechnicalAssurancesTextFields,
)
from osdu_wells.client.data_classes._technical_assurances import (
    _TECHNICALASSURANCES_PROPERTIES_BY_FIELD,
    _create_technical_assurance_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .technical_assurances_acceptable_usage import TechnicalAssurancesAcceptableUsageAPI
from .technical_assurances_reviewers import TechnicalAssurancesReviewersAPI
from .technical_assurances_unacceptable_usage import TechnicalAssurancesUnacceptableUsageAPI
from .technical_assurances_query import TechnicalAssurancesQueryAPI


class TechnicalAssurancesAPI(NodeAPI[TechnicalAssurances, TechnicalAssurancesApply, TechnicalAssurancesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[TechnicalAssurancesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=TechnicalAssurances,
            class_apply_type=TechnicalAssurancesApply,
            class_list=TechnicalAssurancesList,
            class_apply_list=TechnicalAssurancesApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.acceptable_usage_edge = TechnicalAssurancesAcceptableUsageAPI(client)
        self.reviewers_edge = TechnicalAssurancesReviewersAPI(client)
        self.unacceptable_usage_edge = TechnicalAssurancesUnacceptableUsageAPI(client)

    def __call__(
        self,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TechnicalAssurancesQueryAPI[TechnicalAssurancesList]:
        """Query starting at technical assurances.

        Args:
            comment: The comment to filter on.
            comment_prefix: The prefix of the comment to filter on.
            effective_date: The effective date to filter on.
            effective_date_prefix: The prefix of the effective date to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of technical assurances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for technical assurances.

        """
        filter_ = _create_technical_assurance_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            TechnicalAssurancesList,
            [
                QueryStep(
                    name="technical_assurance",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_id, list(_TECHNICALASSURANCES_PROPERTIES_BY_FIELD.values())
                            )
                        ]
                    ),
                    result_cls=TechnicalAssurances,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return TechnicalAssurancesQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, technical_assurance: TechnicalAssurancesApply | Sequence[TechnicalAssurancesApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) technical assurances.

        Note: This method iterates through all nodes and timeseries linked to technical_assurance and creates them including the edges
        between the nodes. For example, if any of `acceptable_usage`, `reviewers` or `unacceptable_usage` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            technical_assurance: Technical assurance or sequence of technical assurances to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new technical_assurance:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import TechnicalAssurancesApply
                >>> client = OSDUClient()
                >>> technical_assurance = TechnicalAssurancesApply(external_id="my_technical_assurance", ...)
                >>> result = client.technical_assurances.apply(technical_assurance)

        """
        return self._apply(technical_assurance, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more technical assurance.

        Args:
            external_id: External id of the technical assurance to delete.
            space: The space where all the technical assurance are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete technical_assurance by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.technical_assurances.delete("my_technical_assurance")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> TechnicalAssurances:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> TechnicalAssurancesList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> TechnicalAssurances | TechnicalAssurancesList:
        """Retrieve one or more technical assurances by id(s).

        Args:
            external_id: External id or list of external ids of the technical assurances.
            space: The space where all the technical assurances are located.

        Returns:
            The requested technical assurances.

        Examples:

            Retrieve technical_assurance by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurance = client.technical_assurances.retrieve("my_technical_assurance")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_pairs=[
                (self.acceptable_usage_edge, "acceptable_usage"),
                (self.reviewers_edge, "reviewers"),
                (self.unacceptable_usage_edge, "unacceptable_usage"),
            ],
        )

    def search(
        self,
        query: str,
        properties: TechnicalAssurancesTextFields | Sequence[TechnicalAssurancesTextFields] | None = None,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TechnicalAssurancesList:
        """Search technical assurances

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            comment: The comment to filter on.
            comment_prefix: The prefix of the comment to filter on.
            effective_date: The effective date to filter on.
            effective_date_prefix: The prefix of the effective date to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of technical assurances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results technical assurances matching the query.

        Examples:

           Search for 'my_technical_assurance' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurances = client.technical_assurances.search('my_technical_assurance')

        """
        filter_ = _create_technical_assurance_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _TECHNICALASSURANCES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: TechnicalAssurancesFields | Sequence[TechnicalAssurancesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: TechnicalAssurancesTextFields | Sequence[TechnicalAssurancesTextFields] | None = None,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
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
        property: TechnicalAssurancesFields | Sequence[TechnicalAssurancesFields] | None = None,
        group_by: TechnicalAssurancesFields | Sequence[TechnicalAssurancesFields] = None,
        query: str | None = None,
        search_properties: TechnicalAssurancesTextFields | Sequence[TechnicalAssurancesTextFields] | None = None,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
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
        property: TechnicalAssurancesFields | Sequence[TechnicalAssurancesFields] | None = None,
        group_by: TechnicalAssurancesFields | Sequence[TechnicalAssurancesFields] | None = None,
        query: str | None = None,
        search_property: TechnicalAssurancesTextFields | Sequence[TechnicalAssurancesTextFields] | None = None,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across technical assurances

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            comment: The comment to filter on.
            comment_prefix: The prefix of the comment to filter on.
            effective_date: The effective date to filter on.
            effective_date_prefix: The prefix of the effective date to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of technical assurances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count technical assurances in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.technical_assurances.aggregate("count", space="my_space")

        """

        filter_ = _create_technical_assurance_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _TECHNICALASSURANCES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: TechnicalAssurancesFields,
        interval: float,
        query: str | None = None,
        search_property: TechnicalAssurancesTextFields | Sequence[TechnicalAssurancesTextFields] | None = None,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for technical assurances

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            comment: The comment to filter on.
            comment_prefix: The prefix of the comment to filter on.
            effective_date: The effective date to filter on.
            effective_date_prefix: The prefix of the effective date to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of technical assurances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_technical_assurance_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _TECHNICALASSURANCES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> TechnicalAssurancesList:
        """List/filter technical assurances

        Args:
            comment: The comment to filter on.
            comment_prefix: The prefix of the comment to filter on.
            effective_date: The effective date to filter on.
            effective_date_prefix: The prefix of the effective date to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of technical assurances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `acceptable_usage`, `reviewers` or `unacceptable_usage` external ids for the technical assurances. Defaults to True.

        Returns:
            List of requested technical assurances

        Examples:

            List technical assurances and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurances = client.technical_assurances.list(limit=5)

        """
        filter_ = _create_technical_assurance_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_pairs=[
                (self.acceptable_usage_edge, "acceptable_usage"),
                (self.reviewers_edge, "reviewers"),
                (self.unacceptable_usage_edge, "unacceptable_usage"),
            ],
        )
