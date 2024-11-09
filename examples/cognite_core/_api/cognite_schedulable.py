from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from cognite_core.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CogniteSchedulable,
    CogniteSchedulableWrite,
    CogniteSchedulableFields,
    CogniteSchedulableList,
    CogniteSchedulableWriteList,
    CogniteSchedulableTextFields,
    CogniteActivity,
)
from cognite_core.data_classes._cognite_schedulable import (
    CogniteSchedulableQuery,
    _COGNITESCHEDULABLE_PROPERTIES_BY_FIELD,
    _create_cognite_schedulable_filter,
)
from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core._api.cognite_schedulable_query import CogniteSchedulableQueryAPI


class CogniteSchedulableAPI(
    NodeAPI[CogniteSchedulable, CogniteSchedulableWrite, CogniteSchedulableList, CogniteSchedulableWriteList]
):
    _view_id = dm.ViewId("cdf_cdm", "CogniteSchedulable", "v1")
    _properties_by_field = _COGNITESCHEDULABLE_PROPERTIES_BY_FIELD
    _direct_children_by_external_id = {
        "CogniteActivity": CogniteActivity,
    }
    _class_type = CogniteSchedulable
    _class_list = CogniteSchedulableList
    _class_write_list = CogniteSchedulableWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogniteSchedulableQueryAPI[CogniteSchedulableList]:
        """Query starting at Cognite schedulables.

        Args:
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_scheduled_end_time: The minimum value of the scheduled end time to filter on.
            max_scheduled_end_time: The maximum value of the scheduled end time to filter on.
            min_scheduled_start_time: The minimum value of the scheduled start time to filter on.
            max_scheduled_start_time: The maximum value of the scheduled start time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite schedulables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite schedulables.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_schedulable_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_scheduled_end_time,
            max_scheduled_end_time,
            min_scheduled_start_time,
            max_scheduled_start_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(CogniteSchedulableList)
        return CogniteSchedulableQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        cognite_schedulable: CogniteSchedulableWrite | Sequence[CogniteSchedulableWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite schedulables.

        Args:
            cognite_schedulable: Cognite schedulable or sequence of Cognite schedulables to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cognite_schedulable:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CogniteSchedulableWrite
                >>> client = CogniteCoreClient()
                >>> cognite_schedulable = CogniteSchedulableWrite(external_id="my_cognite_schedulable", ...)
                >>> result = client.cognite_schedulable.apply(cognite_schedulable)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_schedulable.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_schedulable, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite schedulable.

        Args:
            external_id: External id of the Cognite schedulable to delete.
            space: The space where all the Cognite schedulable are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_schedulable by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_schedulable.delete("my_cognite_schedulable")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_schedulable.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["CogniteActivity"]] | None = None,
    ) -> CogniteSchedulable | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["CogniteActivity"]] | None = None,
    ) -> CogniteSchedulableList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["CogniteActivity"]] | None = None,
    ) -> CogniteSchedulable | CogniteSchedulableList | None:
        """Retrieve one or more Cognite schedulables by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite schedulables.
            space: The space where all the Cognite schedulables are located.
            as_child_class: If you want to retrieve the Cognite schedulables as a child class,
                you can specify the child class here. Note that if one node has properties in
                multiple child classes, you will get duplicate nodes in the result.

        Returns:
            The requested Cognite schedulables.

        Examples:

            Retrieve cognite_schedulable by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_schedulable = client.cognite_schedulable.retrieve("my_cognite_schedulable")

        """
        return self._retrieve(external_id, space, as_child_class=as_child_class)

    def search(
        self,
        query: str,
        properties: CogniteSchedulableTextFields | SequenceNotStr[CogniteSchedulableTextFields] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteSchedulableList:
        """Search Cognite schedulables

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_scheduled_end_time: The minimum value of the scheduled end time to filter on.
            max_scheduled_end_time: The maximum value of the scheduled end time to filter on.
            min_scheduled_start_time: The minimum value of the scheduled start time to filter on.
            max_scheduled_start_time: The maximum value of the scheduled start time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite schedulables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite schedulables matching the query.

        Examples:

           Search for 'my_cognite_schedulable' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_schedulables = client.cognite_schedulable.search('my_cognite_schedulable')

        """
        filter_ = _create_cognite_schedulable_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_scheduled_end_time,
            max_scheduled_end_time,
            min_scheduled_start_time,
            max_scheduled_start_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields],
        property: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        property: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite schedulables

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_scheduled_end_time: The minimum value of the scheduled end time to filter on.
            max_scheduled_end_time: The maximum value of the scheduled end time to filter on.
            min_scheduled_start_time: The minimum value of the scheduled start time to filter on.
            max_scheduled_start_time: The maximum value of the scheduled start time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite schedulables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite schedulables in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_schedulable.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_schedulable_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_scheduled_end_time,
            max_scheduled_end_time,
            min_scheduled_start_time,
            max_scheduled_start_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=None,
            search_properties=None,
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: CogniteSchedulableFields,
        interval: float,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite schedulables

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_scheduled_end_time: The minimum value of the scheduled end time to filter on.
            max_scheduled_end_time: The maximum value of the scheduled end time to filter on.
            min_scheduled_start_time: The minimum value of the scheduled start time to filter on.
            max_scheduled_start_time: The maximum value of the scheduled start time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite schedulables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_schedulable_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_scheduled_end_time,
            max_scheduled_end_time,
            min_scheduled_start_time,
            max_scheduled_start_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            None,
            None,
            limit,
            filter_,
        )

    def query(self) -> CogniteSchedulableQuery:
        """Start a query for Cognite schedulables."""
        warnings.warn("The .query is in alpha and is subject to breaking changes without notice.")
        return CogniteSchedulableQuery(self._client)

    def list(
        self,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteSchedulableFields | Sequence[CogniteSchedulableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteSchedulableList:
        """List/filter Cognite schedulables

        Args:
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_scheduled_end_time: The minimum value of the scheduled end time to filter on.
            max_scheduled_end_time: The maximum value of the scheduled end time to filter on.
            min_scheduled_start_time: The minimum value of the scheduled start time to filter on.
            max_scheduled_start_time: The maximum value of the scheduled start time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite schedulables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested Cognite schedulables

        Examples:

            List Cognite schedulables and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_schedulables = client.cognite_schedulable.list(limit=5)

        """
        filter_ = _create_cognite_schedulable_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_scheduled_end_time,
            max_scheduled_end_time,
            min_scheduled_start_time,
            max_scheduled_start_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )
