from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Acl,
    AclApply,
    AclFields,
    AclList,
    AclApplyList,
    AclTextFields,
)
from osdu_wells.client.data_classes._acl import (
    _ACL_PROPERTIES_BY_FIELD,
    _create_acl_filter,
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
from .acl_query import AclQueryAPI


class AclAPI(NodeAPI[Acl, AclApply, AclList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AclApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Acl,
            class_apply_type=AclApply,
            class_list=AclList,
            class_apply_list=AclApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> AclQueryAPI[AclList]:
        """Query starting at acls.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of acls to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for acls.

        """
        filter_ = _create_acl_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            AclList,
            [
                QueryStep(
                    name="acl",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_ACL_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Acl,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return AclQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, acl: AclApply | Sequence[AclApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) acls.

        Args:
            acl: Acl or sequence of acls to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new acl:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import AclApply
                >>> client = OSDUClient()
                >>> acl = AclApply(external_id="my_acl", ...)
                >>> result = client.acl.apply(acl)

        """
        return self._apply(acl, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more acl.

        Args:
            external_id: External id of the acl to delete.
            space: The space where all the acl are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete acl by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.acl.delete("my_acl")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Acl | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> AclList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> Acl | AclList | None:
        """Retrieve one or more acls by id(s).

        Args:
            external_id: External id or list of external ids of the acls.
            space: The space where all the acls are located.

        Returns:
            The requested acls.

        Examples:

            Retrieve acl by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> acl = client.acl.retrieve("my_acl")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: AclTextFields | Sequence[AclTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AclList:
        """Search acls

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of acls to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results acls matching the query.

        Examples:

           Search for 'my_acl' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> acls = client.acl.search('my_acl')

        """
        filter_ = _create_acl_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _ACL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AclFields | Sequence[AclFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AclTextFields | Sequence[AclTextFields] | None = None,
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
        property: AclFields | Sequence[AclFields] | None = None,
        group_by: AclFields | Sequence[AclFields] = None,
        query: str | None = None,
        search_properties: AclTextFields | Sequence[AclTextFields] | None = None,
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
        property: AclFields | Sequence[AclFields] | None = None,
        group_by: AclFields | Sequence[AclFields] | None = None,
        query: str | None = None,
        search_property: AclTextFields | Sequence[AclTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across acls

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of acls to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count acls in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.acl.aggregate("count", space="my_space")

        """

        filter_ = _create_acl_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ACL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AclFields,
        interval: float,
        query: str | None = None,
        search_property: AclTextFields | Sequence[AclTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for acls

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of acls to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_acl_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ACL_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AclList:
        """List/filter acls

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of acls to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested acls

        Examples:

            List acls and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> acls = client.acl.list(limit=5)

        """
        filter_ = _create_acl_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
