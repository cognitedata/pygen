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
    CDFExternalReferencesListable,
    CDFExternalReferencesListableApply,
    CDFExternalReferencesListableFields,
    CDFExternalReferencesListableList,
    CDFExternalReferencesListableApplyList,
)
from omni.data_classes._cdf_external_references_listable import (
    _CDFEXTERNALREFERENCESLISTABLE_PROPERTIES_BY_FIELD,
    _create_cdf_external_references_listable_filter,
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
from .cdf_external_references_listable_timeseries import CDFExternalReferencesListableTimeseriesAPI
from .cdf_external_references_listable_query import CDFExternalReferencesListableQueryAPI


class CDFExternalReferencesListableAPI(
    NodeAPI[CDFExternalReferencesListable, CDFExternalReferencesListableApply, CDFExternalReferencesListableList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CDFExternalReferencesListableApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CDFExternalReferencesListable,
            class_apply_type=CDFExternalReferencesListableApply,
            class_list=CDFExternalReferencesListableList,
            class_apply_list=CDFExternalReferencesListableApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.timeseries = CDFExternalReferencesListableTimeseriesAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CDFExternalReferencesListableQueryAPI[CDFExternalReferencesListableList]:
        """Query starting at cdf external references listables.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for cdf external references listables.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cdf_external_references_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(CDFExternalReferencesListableList)
        return CDFExternalReferencesListableQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self,
        cdf_external_references_listable: CDFExternalReferencesListableApply
        | Sequence[CDFExternalReferencesListableApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) cdf external references listables.

        Args:
            cdf_external_references_listable: Cdf external references listable or sequence of cdf external references listables to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cdf_external_references_listable:

                >>> from omni import OmniClient
                >>> from omni.data_classes import CDFExternalReferencesListableApply
                >>> client = OmniClient()
                >>> cdf_external_references_listable = CDFExternalReferencesListableApply(external_id="my_cdf_external_references_listable", ...)
                >>> result = client.cdf_external_references_listable.apply(cdf_external_references_listable)

        """
        return self._apply(cdf_external_references_listable, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more cdf external references listable.

        Args:
            external_id: External id of the cdf external references listable to delete.
            space: The space where all the cdf external references listable are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cdf_external_references_listable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.cdf_external_references_listable.delete("my_cdf_external_references_listable")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> CDFExternalReferencesListable | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CDFExternalReferencesListableList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CDFExternalReferencesListable | CDFExternalReferencesListableList | None:
        """Retrieve one or more cdf external references listables by id(s).

        Args:
            external_id: External id or list of external ids of the cdf external references listables.
            space: The space where all the cdf external references listables are located.

        Returns:
            The requested cdf external references listables.

        Examples:

            Retrieve cdf_external_references_listable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_references_listable = client.cdf_external_references_listable.retrieve("my_cdf_external_references_listable")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CDFExternalReferencesListableFields | Sequence[CDFExternalReferencesListableFields] | None = None,
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
        property: CDFExternalReferencesListableFields | Sequence[CDFExternalReferencesListableFields] | None = None,
        group_by: CDFExternalReferencesListableFields | Sequence[CDFExternalReferencesListableFields] = None,
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
        property: CDFExternalReferencesListableFields | Sequence[CDFExternalReferencesListableFields] | None = None,
        group_by: CDFExternalReferencesListableFields | Sequence[CDFExternalReferencesListableFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across cdf external references listables

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cdf external references listables in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.cdf_external_references_listable.aggregate("count", space="my_space")

        """

        filter_ = _create_cdf_external_references_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _CDFEXTERNALREFERENCESLISTABLE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CDFExternalReferencesListableFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cdf external references listables

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cdf_external_references_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _CDFEXTERNALREFERENCESLISTABLE_PROPERTIES_BY_FIELD,
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
    ) -> CDFExternalReferencesListableList:
        """List/filter cdf external references listables

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested cdf external references listables

        Examples:

            List cdf external references listables and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_references_listables = client.cdf_external_references_listable.list(limit=5)

        """
        filter_ = _create_cdf_external_references_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
