from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from wind_turbine._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from wind_turbine.data_classes._data_sheet import (
    DataSheetQuery,
    _DATASHEET_PROPERTIES_BY_FIELD,
    _create_data_sheet_filter,
)
from wind_turbine.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    DataSheet,
    DataSheetWrite,
    DataSheetFields,
    DataSheetList,
    DataSheetWriteList,
    DataSheetTextFields,
)
from wind_turbine._api.data_sheet_query import DataSheetQueryAPI


class DataSheetAPI(NodeAPI[DataSheet, DataSheetWrite, DataSheetList, DataSheetWriteList]):
    _view_id = dm.ViewId("sp_pygen_power", "DataSheet", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _DATASHEET_PROPERTIES_BY_FIELD
    _class_type = DataSheet
    _class_list = DataSheetList
    _class_write_list = DataSheetWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> DataSheetQueryAPI[DataSheetList]:
        """Query starting at data sheets.

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            directory: The directory to filter on.
            directory_prefix: The prefix of the directory to filter on.
            is_uploaded: The is uploaded to filter on.
            mime_type: The mime type to filter on.
            mime_type_prefix: The prefix of the mime type to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_uploaded_time: The minimum value of the uploaded time to filter on.
            max_uploaded_time: The maximum value of the uploaded time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of data sheets to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for data sheets.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_data_sheet_filter(
            self._view_id,
            description,
            description_prefix,
            directory,
            directory_prefix,
            is_uploaded,
            mime_type,
            mime_type_prefix,
            name,
            name_prefix,
            min_uploaded_time,
            max_uploaded_time,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(DataSheetList)
        return DataSheetQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        data_sheet: DataSheetWrite | Sequence[DataSheetWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) data sheets.

        Args:
            data_sheet: Data sheet or
                sequence of data sheets to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None.
                However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new data_sheet:

                >>> from wind_turbine import WindTurbineClient
                >>> from wind_turbine.data_classes import DataSheetWrite
                >>> client = WindTurbineClient()
                >>> data_sheet = DataSheetWrite(
                ...     external_id="my_data_sheet", ...
                ... )
                >>> result = client.data_sheet.apply(data_sheet)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.data_sheet.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(data_sheet, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more data sheet.

        Args:
            external_id: External id of the data sheet to delete.
            space: The space where all the data sheet are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete data_sheet by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> client.data_sheet.delete("my_data_sheet")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.data_sheet.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> DataSheet | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> DataSheetList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> DataSheet | DataSheetList | None:
        """Retrieve one or more data sheets by id(s).

        Args:
            external_id: External id or list of external ids of the data sheets.
            space: The space where all the data sheets are located.

        Returns:
            The requested data sheets.

        Examples:

            Retrieve data_sheet by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> data_sheet = client.data_sheet.retrieve(
                ...     "my_data_sheet"
                ... )

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: DataSheetTextFields | SequenceNotStr[DataSheetTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: DataSheetFields | SequenceNotStr[DataSheetFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> DataSheetList:
        """Search data sheets

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            directory: The directory to filter on.
            directory_prefix: The prefix of the directory to filter on.
            is_uploaded: The is uploaded to filter on.
            mime_type: The mime type to filter on.
            mime_type_prefix: The prefix of the mime type to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_uploaded_time: The minimum value of the uploaded time to filter on.
            max_uploaded_time: The maximum value of the uploaded time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of data sheets to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results data sheets matching the query.

        Examples:

           Search for 'my_data_sheet' in all text properties:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> data_sheets = client.data_sheet.search(
                ...     'my_data_sheet'
                ... )

        """
        filter_ = _create_data_sheet_filter(
            self._view_id,
            description,
            description_prefix,
            directory,
            directory_prefix,
            is_uploaded,
            mime_type,
            mime_type_prefix,
            name,
            name_prefix,
            min_uploaded_time,
            max_uploaded_time,
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
        property: DataSheetFields | SequenceNotStr[DataSheetFields] | None = None,
        query: str | None = None,
        search_property: DataSheetTextFields | SequenceNotStr[DataSheetTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
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
        property: DataSheetFields | SequenceNotStr[DataSheetFields] | None = None,
        query: str | None = None,
        search_property: DataSheetTextFields | SequenceNotStr[DataSheetTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
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
        group_by: DataSheetFields | SequenceNotStr[DataSheetFields],
        property: DataSheetFields | SequenceNotStr[DataSheetFields] | None = None,
        query: str | None = None,
        search_property: DataSheetTextFields | SequenceNotStr[DataSheetTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
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
        group_by: DataSheetFields | SequenceNotStr[DataSheetFields] | None = None,
        property: DataSheetFields | SequenceNotStr[DataSheetFields] | None = None,
        query: str | None = None,
        search_property: DataSheetTextFields | SequenceNotStr[DataSheetTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across data sheets

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            directory: The directory to filter on.
            directory_prefix: The prefix of the directory to filter on.
            is_uploaded: The is uploaded to filter on.
            mime_type: The mime type to filter on.
            mime_type_prefix: The prefix of the mime type to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_uploaded_time: The minimum value of the uploaded time to filter on.
            max_uploaded_time: The maximum value of the uploaded time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of data sheets to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count data sheets in space `my_space`:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> result = client.data_sheet.aggregate("count", space="my_space")

        """

        filter_ = _create_data_sheet_filter(
            self._view_id,
            description,
            description_prefix,
            directory,
            directory_prefix,
            is_uploaded,
            mime_type,
            mime_type_prefix,
            name,
            name_prefix,
            min_uploaded_time,
            max_uploaded_time,
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
        property: DataSheetFields,
        interval: float,
        query: str | None = None,
        search_property: DataSheetTextFields | SequenceNotStr[DataSheetTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for data sheets

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            directory: The directory to filter on.
            directory_prefix: The prefix of the directory to filter on.
            is_uploaded: The is uploaded to filter on.
            mime_type: The mime type to filter on.
            mime_type_prefix: The prefix of the mime type to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_uploaded_time: The minimum value of the uploaded time to filter on.
            max_uploaded_time: The maximum value of the uploaded time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of data sheets to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_data_sheet_filter(
            self._view_id,
            description,
            description_prefix,
            directory,
            directory_prefix,
            is_uploaded,
            mime_type,
            mime_type_prefix,
            name,
            name_prefix,
            min_uploaded_time,
            max_uploaded_time,
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

    def query(self) -> DataSheetQuery:
        """Start a query for data sheets."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return DataSheetQuery(self._client)

    def select(self) -> DataSheetQuery:
        """Start selecting from data sheets."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return DataSheetQuery(self._client)

    def list(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: DataSheetFields | Sequence[DataSheetFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> DataSheetList:
        """List/filter data sheets

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            directory: The directory to filter on.
            directory_prefix: The prefix of the directory to filter on.
            is_uploaded: The is uploaded to filter on.
            mime_type: The mime type to filter on.
            mime_type_prefix: The prefix of the mime type to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_uploaded_time: The minimum value of the uploaded time to filter on.
            max_uploaded_time: The maximum value of the uploaded time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of data sheets to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested data sheets

        Examples:

            List data sheets and limit to 5:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> data_sheets = client.data_sheet.list(limit=5)

        """
        filter_ = _create_data_sheet_filter(
            self._view_id,
            description,
            description_prefix,
            directory,
            directory_prefix,
            is_uploaded,
            mime_type,
            mime_type_prefix,
            name,
            name_prefix,
            min_uploaded_time,
            max_uploaded_time,
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
