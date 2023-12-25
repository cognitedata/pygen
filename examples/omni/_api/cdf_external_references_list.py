from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from omni.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    CDFExternalReferencesList,
    CDFExternalReferencesListApply,
    CDFExternalReferencesListFields,
    CDFExternalReferencesListList,
    CDFExternalReferencesListApplyList,
)
from omni.data_classes._cdf_external_references_list import (
    _CDFEXTERNALREFERENCESLIST_PROPERTIES_BY_FIELD,
    _create_cdf_external_references_list_filter,
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
from .cdf_external_references_list_timeseries import CDFExternalReferencesListTimeseriesAPI
from .cdf_external_references_list_query import CDFExternalReferencesListQueryAPI


class CDFExternalReferencesListAPI(
    NodeAPI[CDFExternalReferencesList, CDFExternalReferencesListApply, CDFExternalReferencesListList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CDFExternalReferencesListApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CDFExternalReferencesList,
            class_apply_type=CDFExternalReferencesListApply,
            class_list=CDFExternalReferencesListList,
            class_apply_list=CDFExternalReferencesListApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.timeseries = CDFExternalReferencesListTimeseriesAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CDFExternalReferencesListQueryAPI[CDFExternalReferencesListList]:
        """Query starting at cdf external references lists.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references lists to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for cdf external references lists.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cdf_external_references_list_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(CDFExternalReferencesListList)
        return CDFExternalReferencesListQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self,
        cdf_external_references_list: CDFExternalReferencesListApply | Sequence[CDFExternalReferencesListApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) cdf external references lists.

        Args:
            cdf_external_references_list: Cdf external references list or sequence of cdf external references lists to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cdf_external_references_list:

                >>> from omni import OmniClient
                >>> from omni.data_classes import CDFExternalReferencesListApply
                >>> client = OmniClient()
                >>> cdf_external_references_list = CDFExternalReferencesListApply(external_id="my_cdf_external_references_list", ...)
                >>> result = client.cdf_external_references_list.apply(cdf_external_references_list)

        """
        return self._apply(cdf_external_references_list, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more cdf external references list.

        Args:
            external_id: External id of the cdf external references list to delete.
            space: The space where all the cdf external references list are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cdf_external_references_list by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.cdf_external_references_list.delete("my_cdf_external_references_list")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> CDFExternalReferencesList | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CDFExternalReferencesListList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CDFExternalReferencesList | CDFExternalReferencesListList | None:
        """Retrieve one or more cdf external references lists by id(s).

        Args:
            external_id: External id or list of external ids of the cdf external references lists.
            space: The space where all the cdf external references lists are located.

        Returns:
            The requested cdf external references lists.

        Examples:

            Retrieve cdf_external_references_list by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_references_list = client.cdf_external_references_list.retrieve("my_cdf_external_references_list")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CDFExternalReferencesListFields | Sequence[CDFExternalReferencesListFields] | None = None,
        group_by: None = None,
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
        property: CDFExternalReferencesListFields | Sequence[CDFExternalReferencesListFields] | None = None,
        group_by: CDFExternalReferencesListFields | Sequence[CDFExternalReferencesListFields] = None,
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
        property: CDFExternalReferencesListFields | Sequence[CDFExternalReferencesListFields] | None = None,
        group_by: CDFExternalReferencesListFields | Sequence[CDFExternalReferencesListFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across cdf external references lists

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references lists to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cdf external references lists in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.cdf_external_references_list.aggregate("count", space="my_space")

        """

        filter_ = _create_cdf_external_references_list_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _CDFEXTERNALREFERENCESLIST_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CDFExternalReferencesListFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cdf external references lists

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references lists to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cdf_external_references_list_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _CDFEXTERNALREFERENCESLIST_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CDFExternalReferencesListList:
        """List/filter cdf external references lists

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references lists to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested cdf external references lists

        Examples:

            List cdf external references lists and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_references_lists = client.cdf_external_references_list.list(limit=5)

        """
        filter_ = _create_cdf_external_references_list_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
