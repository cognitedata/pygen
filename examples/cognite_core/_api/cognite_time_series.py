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
    CogniteTimeSeries,
    CogniteTimeSeriesWrite,
    CogniteTimeSeriesFields,
    CogniteTimeSeriesList,
    CogniteTimeSeriesWriteList,
    CogniteTimeSeriesTextFields,
    CogniteActivity,
    CogniteAsset,
    CogniteEquipment,
    CogniteSourceSystem,
    CogniteUnit,
)
from cognite_core.data_classes._cognite_time_series import (
    CogniteTimeSeriesQuery,
    _COGNITETIMESERIES_PROPERTIES_BY_FIELD,
    _create_cognite_time_series_filter,
)
from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core._api.cognite_time_series_query import CogniteTimeSeriesQueryAPI


class CogniteTimeSeriesAPI(
    NodeAPI[CogniteTimeSeries, CogniteTimeSeriesWrite, CogniteTimeSeriesList, CogniteTimeSeriesWriteList]
):
    _view_id = dm.ViewId("cdf_cdm", "CogniteTimeSeries", "v1")
    _properties_by_field = _COGNITETIMESERIES_PROPERTIES_BY_FIELD
    _class_type = CogniteTimeSeries
    _class_list = CogniteTimeSeriesList
    _class_write_list = CogniteTimeSeriesWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        unit: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogniteTimeSeriesQueryAPI[CogniteTimeSeriesList]:
        """Query starting at Cognite time series.

        Args:
            assets: The asset to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment: The equipment to filter on.
            is_step: The is step to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            source_unit: The source unit to filter on.
            source_unit_prefix: The prefix of the source unit to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            unit: The unit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite time series.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_time_series_filter(
            self._view_id,
            assets,
            description,
            description_prefix,
            equipment,
            is_step,
            name,
            name_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            source_unit,
            source_unit_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            unit,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(CogniteTimeSeriesList)
        return CogniteTimeSeriesQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        cognite_time_series: CogniteTimeSeriesWrite | Sequence[CogniteTimeSeriesWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite time series.

        Note: This method iterates through all nodes and timeseries linked to cognite_time_series and creates them including the edges
        between the nodes. For example, if any of `assets`, `equipment`, `source` or `unit` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            cognite_time_series: Cognite time series or sequence of Cognite time series to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cognite_time_series:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CogniteTimeSeriesWrite
                >>> client = CogniteCoreClient()
                >>> cognite_time_series = CogniteTimeSeriesWrite(external_id="my_cognite_time_series", ...)
                >>> result = client.cognite_time_series.apply(cognite_time_series)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_time_series.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_time_series, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite time series.

        Args:
            external_id: External id of the Cognite time series to delete.
            space: The space where all the Cognite time series are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_time_series by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_time_series.delete("my_cognite_time_series")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_time_series.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CogniteTimeSeries | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CogniteTimeSeriesList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteTimeSeries | CogniteTimeSeriesList | None:
        """Retrieve one or more Cognite time series by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite time series.
            space: The space where all the Cognite time series are located.

        Returns:
            The requested Cognite time series.

        Examples:

            Retrieve cognite_time_series by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_time_series = client.cognite_time_series.retrieve("my_cognite_time_series")

        """
        return self._retrieve(external_id, space, retrieve_edges=True, edge_api_name_type_direction_view_id_penta=[])

    def search(
        self,
        query: str,
        properties: CogniteTimeSeriesTextFields | SequenceNotStr[CogniteTimeSeriesTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        unit: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteTimeSeriesFields | SequenceNotStr[CogniteTimeSeriesFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteTimeSeriesList:
        """Search Cognite time series

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            assets: The asset to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment: The equipment to filter on.
            is_step: The is step to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            source_unit: The source unit to filter on.
            source_unit_prefix: The prefix of the source unit to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            unit: The unit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite time series matching the query.

        Examples:

           Search for 'my_cognite_time_series' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_time_series_list = client.cognite_time_series.search('my_cognite_time_series')

        """
        filter_ = _create_cognite_time_series_filter(
            self._view_id,
            assets,
            description,
            description_prefix,
            equipment,
            is_step,
            name,
            name_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            source_unit,
            source_unit_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            unit,
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
        property: CogniteTimeSeriesFields | SequenceNotStr[CogniteTimeSeriesFields] | None = None,
        query: str | None = None,
        search_property: CogniteTimeSeriesTextFields | SequenceNotStr[CogniteTimeSeriesTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        unit: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        property: CogniteTimeSeriesFields | SequenceNotStr[CogniteTimeSeriesFields] | None = None,
        query: str | None = None,
        search_property: CogniteTimeSeriesTextFields | SequenceNotStr[CogniteTimeSeriesTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        unit: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: CogniteTimeSeriesFields | SequenceNotStr[CogniteTimeSeriesFields],
        property: CogniteTimeSeriesFields | SequenceNotStr[CogniteTimeSeriesFields] | None = None,
        query: str | None = None,
        search_property: CogniteTimeSeriesTextFields | SequenceNotStr[CogniteTimeSeriesTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        unit: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: CogniteTimeSeriesFields | SequenceNotStr[CogniteTimeSeriesFields] | None = None,
        property: CogniteTimeSeriesFields | SequenceNotStr[CogniteTimeSeriesFields] | None = None,
        query: str | None = None,
        search_property: CogniteTimeSeriesTextFields | SequenceNotStr[CogniteTimeSeriesTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        unit: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite time series

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            assets: The asset to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment: The equipment to filter on.
            is_step: The is step to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            source_unit: The source unit to filter on.
            source_unit_prefix: The prefix of the source unit to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            unit: The unit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite time series in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_time_series.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_time_series_filter(
            self._view_id,
            assets,
            description,
            description_prefix,
            equipment,
            is_step,
            name,
            name_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            source_unit,
            source_unit_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            unit,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: CogniteTimeSeriesFields,
        interval: float,
        query: str | None = None,
        search_property: CogniteTimeSeriesTextFields | SequenceNotStr[CogniteTimeSeriesTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        unit: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite time series

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            assets: The asset to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment: The equipment to filter on.
            is_step: The is step to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            source_unit: The source unit to filter on.
            source_unit_prefix: The prefix of the source unit to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            unit: The unit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_time_series_filter(
            self._view_id,
            assets,
            description,
            description_prefix,
            equipment,
            is_step,
            name,
            name_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            source_unit,
            source_unit_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            unit,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def query(self) -> CogniteTimeSeriesQuery:
        """Start a query for Cognite time series."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return CogniteTimeSeriesQuery(self._client)

    def select(self) -> CogniteTimeSeriesQuery:
        """Start selecting from Cognite time series."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return CogniteTimeSeriesQuery(self._client)

    def list(
        self,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        unit: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteTimeSeriesFields | Sequence[CogniteTimeSeriesFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CogniteTimeSeriesList:
        """List/filter Cognite time series

        Args:
            assets: The asset to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment: The equipment to filter on.
            is_step: The is step to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            source_unit: The source unit to filter on.
            source_unit_prefix: The prefix of the source unit to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            unit: The unit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `activities`, `assets`, `equipment`, `source` and `unit` for the Cognite time series. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested Cognite time series

        Examples:

            List Cognite time series and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_time_series_list = client.cognite_time_series.list(limit=5)

        """
        filter_ = _create_cognite_time_series_filter(
            self._view_id,
            assets,
            description,
            description_prefix,
            equipment,
            is_step,
            name,
            name_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            source_unit,
            source_unit_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            unit,
            external_id_prefix,
            space,
            filter,
        )

        if retrieve_connections == "skip":
            return self._list(
                limit=limit,
                filter=filter_,
                sort_by=sort_by,  # type: ignore[arg-type]
                direction=direction,
                sort=sort,
            )

        builder = DataClassQueryBuilder(CogniteTimeSeriesList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                CogniteTimeSeries,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteActivity._view_id]),
                        direction="inwards",
                        through=CogniteActivity._view_id.as_property_ref("timeSeries"),
                    ),
                    CogniteActivity,
                    connection_type="reverse-list",
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteAsset._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("assets"),
                    ),
                    CogniteAsset,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteEquipment._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("equipment"),
                    ),
                    CogniteEquipment,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteSourceSystem._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("source"),
                    ),
                    CogniteSourceSystem,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteUnit._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("unit"),
                    ),
                    CogniteUnit,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
