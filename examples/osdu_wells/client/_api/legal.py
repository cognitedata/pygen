from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Legal,
    LegalApply,
    LegalFields,
    LegalList,
    LegalTextFields,
)
from osdu_wells.client.data_classes._legal import (
    _LEGAL_PROPERTIES_BY_FIELD,
    _create_legal_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .legal_query import LegalQueryAPI


class LegalAPI(NodeAPI[Legal, LegalApply, LegalList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[LegalApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Legal,
            class_apply_type=LegalApply,
            class_list=LegalList,
            class_apply_list=LegalApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LegalQueryAPI[LegalList]:
        """Query starting at legals.

        Args:
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of legals to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for legals.

        """
        filter_ = _create_legal_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            LegalList,
            [
                QueryStep(
                    name="legal",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_LEGAL_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Legal,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return LegalQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, legal: LegalApply | Sequence[LegalApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) legals.

        Args:
            legal: Legal or sequence of legals to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new legal:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import LegalApply
                >>> client = OSDUClient()
                >>> legal = LegalApply(external_id="my_legal", ...)
                >>> result = client.legal.apply(legal)

        """
        return self._apply(legal, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more legal.

        Args:
            external_id: External id of the legal to delete.
            space: The space where all the legal are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete legal by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.legal.delete("my_legal")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Legal:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> LegalList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> Legal | LegalList:
        """Retrieve one or more legals by id(s).

        Args:
            external_id: External id or list of external ids of the legals.
            space: The space where all the legals are located.

        Returns:
            The requested legals.

        Examples:

            Retrieve legal by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> legal = client.legal.retrieve("my_legal")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LegalList:
        """Search legals

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of legals to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results legals matching the query.

        Examples:

           Search for 'my_legal' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> legals = client.legal.search('my_legal')

        """
        filter_ = _create_legal_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _LEGAL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: LegalFields | Sequence[LegalFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
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
        property: LegalFields | Sequence[LegalFields] | None = None,
        group_by: LegalFields | Sequence[LegalFields] = None,
        query: str | None = None,
        search_properties: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
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
        property: LegalFields | Sequence[LegalFields] | None = None,
        group_by: LegalFields | Sequence[LegalFields] | None = None,
        query: str | None = None,
        search_property: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across legals

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of legals to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count legals in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.legal.aggregate("count", space="my_space")

        """

        filter_ = _create_legal_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _LEGAL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: LegalFields,
        interval: float,
        query: str | None = None,
        search_property: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for legals

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of legals to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_legal_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _LEGAL_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LegalList:
        """List/filter legals

        Args:
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of legals to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested legals

        Examples:

            List legals and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> legals = client.legal.list(limit=5)

        """
        filter_ = _create_legal_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
