from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from omni.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    CDFExternalReferencesListed,
    CDFExternalReferencesListedApply,
    CDFExternalReferencesListedFields,
    CDFExternalReferencesListedList,
    CDFExternalReferencesListedApplyList,
)
from omni.data_classes._cdf_external_references_listed import (
    _CDFEXTERNALREFERENCESLISTED_PROPERTIES_BY_FIELD,
    _create_cdf_external_references_listed_filter,
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
from .cdf_external_references_listed_timeseries import CDFExternalReferencesListedTimeseriesAPI
from .cdf_external_references_listed_query import CDFExternalReferencesListedQueryAPI


class CDFExternalReferencesListedAPI(
    NodeAPI[CDFExternalReferencesListed, CDFExternalReferencesListedApply, CDFExternalReferencesListedList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[CDFExternalReferencesListed]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CDFExternalReferencesListed,
            class_list=CDFExternalReferencesListedList,
            class_apply_list=CDFExternalReferencesListedApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.timeseries = CDFExternalReferencesListedTimeseriesAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CDFExternalReferencesListedQueryAPI[CDFExternalReferencesListedList]:
        """Query starting at cdf external references listeds.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for cdf external references listeds.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cdf_external_references_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(CDFExternalReferencesListedList)
        return CDFExternalReferencesListedQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        cdf_external_references_listed: CDFExternalReferencesListedApply | Sequence[CDFExternalReferencesListedApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) cdf external references listeds.

        Args:
            cdf_external_references_listed: Cdf external references listed or sequence of cdf external references listeds to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cdf_external_references_listed:

                >>> from omni import OmniClient
                >>> from omni.data_classes import CDFExternalReferencesListedApply
                >>> client = OmniClient()
                >>> cdf_external_references_listed = CDFExternalReferencesListedApply(external_id="my_cdf_external_references_listed", ...)
                >>> result = client.cdf_external_references_listed.apply(cdf_external_references_listed)

        """
        return self._apply(cdf_external_references_listed, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more cdf external references listed.

        Args:
            external_id: External id of the cdf external references listed to delete.
            space: The space where all the cdf external references listed are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cdf_external_references_listed by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.cdf_external_references_listed.delete("my_cdf_external_references_listed")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> CDFExternalReferencesListed | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CDFExternalReferencesListedList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CDFExternalReferencesListed | CDFExternalReferencesListedList | None:
        """Retrieve one or more cdf external references listeds by id(s).

        Args:
            external_id: External id or list of external ids of the cdf external references listeds.
            space: The space where all the cdf external references listeds are located.

        Returns:
            The requested cdf external references listeds.

        Examples:

            Retrieve cdf_external_references_listed by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_references_listed = client.cdf_external_references_listed.retrieve("my_cdf_external_references_listed")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] | None = None,
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
        property: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] | None = None,
        group_by: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] = None,
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
        property: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] | None = None,
        group_by: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across cdf external references listeds

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cdf external references listeds in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.cdf_external_references_listed.aggregate("count", space="my_space")

        """

        filter_ = _create_cdf_external_references_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _CDFEXTERNALREFERENCESLISTED_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CDFExternalReferencesListedFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cdf external references listeds

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cdf_external_references_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _CDFEXTERNALREFERENCESLISTED_PROPERTIES_BY_FIELD,
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
    ) -> CDFExternalReferencesListedList:
        """List/filter cdf external references listeds

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested cdf external references listeds

        Examples:

            List cdf external references listeds and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_references_listeds = client.cdf_external_references_listed.list(limit=5)

        """
        filter_ = _create_cdf_external_references_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
