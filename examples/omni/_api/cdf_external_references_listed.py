from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CDFExternalReferencesListed,
    CDFExternalReferencesListedWrite,
    CDFExternalReferencesListedFields,
    CDFExternalReferencesListedList,
    CDFExternalReferencesListedWriteList,
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
    NodeAPI[CDFExternalReferencesListed, CDFExternalReferencesListedWrite, CDFExternalReferencesListedList]
):
    _view_id = dm.ViewId("pygen-models", "CDFExternalReferencesListed", "1")

    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            class_type=CDFExternalReferencesListed,
            class_list=CDFExternalReferencesListedList,
            class_write_list=CDFExternalReferencesListedWriteList,
        )
        self.timeseries = CDFExternalReferencesListedTimeseriesAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
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
        return CDFExternalReferencesListedQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        cdf_external_references_listed: CDFExternalReferencesListedWrite | Sequence[CDFExternalReferencesListedWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) cdf external references listeds.

        Args:
            cdf_external_references_listed: Cdf external references listed or sequence of cdf external references listeds to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cdf_external_references_listed:

                >>> from omni import OmniClient
                >>> from omni.data_classes import CDFExternalReferencesListedWrite
                >>> client = OmniClient()
                >>> cdf_external_references_listed = CDFExternalReferencesListedWrite(external_id="my_cdf_external_references_listed", ...)
                >>> result = client.cdf_external_references_listed.apply(cdf_external_references_listed)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cdf_external_references_listed.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cdf_external_references_listed, replace, write_none)

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
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cdf_external_references_listed.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> CDFExternalReferencesListed | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CDFExternalReferencesListedList: ...

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
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] | None = None,
        group_by: None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] | None = None,
        group_by: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] | None = None,
        group_by: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
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
        limit: int | None = DEFAULT_LIMIT_READ,
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
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CDFExternalReferencesListedList:
        """List/filter cdf external references listeds

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

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
        return self._list(
            limit=limit,
            filter=filter_,
            properties_by_field=_CDFEXTERNALREFERENCESLISTED_PROPERTIES_BY_FIELD,
            sort_by=sort_by,
            direction=direction,
            sort=sort,
        )
