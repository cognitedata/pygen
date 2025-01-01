from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite_core.data_classes._cognite_equipment import (
    CogniteEquipmentQuery,
    _COGNITEEQUIPMENT_PROPERTIES_BY_FIELD,
    _create_cognite_equipment_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CogniteEquipment,
    CogniteEquipmentWrite,
    CogniteEquipmentFields,
    CogniteEquipmentList,
    CogniteEquipmentWriteList,
    CogniteEquipmentTextFields,
    CogniteActivity,
    CogniteAsset,
    CogniteEquipmentType,
    CogniteFile,
    CogniteSourceSystem,
    CogniteTimeSeries,
)
from cognite_core._api.cognite_equipment_query import CogniteEquipmentQueryAPI


class CogniteEquipmentAPI(
    NodeAPI[CogniteEquipment, CogniteEquipmentWrite, CogniteEquipmentList, CogniteEquipmentWriteList]
):
    _view_id = dm.ViewId("cdf_cdm", "CogniteEquipment", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITEEQUIPMENT_PROPERTIES_BY_FIELD
    _class_type = CogniteEquipment
    _class_list = CogniteEquipmentList
    _class_write_list = CogniteEquipmentWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        asset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_type: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        files: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        serial_number: str | list[str] | None = None,
        serial_number_prefix: str | None = None,
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
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogniteEquipmentQueryAPI[CogniteEquipment, CogniteEquipmentList]:
        """Query starting at Cognite equipments.

        Args:
            asset: The asset to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment_type: The equipment type to filter on.
            files: The file to filter on.
            manufacturer: The manufacturer to filter on.
            manufacturer_prefix: The prefix of the manufacturer to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            serial_number: The serial number to filter on.
            serial_number_prefix: The prefix of the serial number to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite equipments to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite equipments.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_equipment_filter(
            self._view_id,
            asset,
            description,
            description_prefix,
            equipment_type,
            files,
            manufacturer,
            manufacturer_prefix,
            name,
            name_prefix,
            serial_number,
            serial_number_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return CogniteEquipmentQueryAPI(
            self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit
        )

    def apply(
        self,
        cognite_equipment: CogniteEquipmentWrite | Sequence[CogniteEquipmentWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite equipments.

        Args:
            cognite_equipment: Cognite equipment or
                sequence of Cognite equipments to upsert.
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

            Create a new cognite_equipment:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CogniteEquipmentWrite
                >>> client = CogniteCoreClient()
                >>> cognite_equipment = CogniteEquipmentWrite(
                ...     external_id="my_cognite_equipment", ...
                ... )
                >>> result = client.cognite_equipment.apply(cognite_equipment)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_equipment.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_equipment, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite equipment.

        Args:
            external_id: External id of the Cognite equipment to delete.
            space: The space where all the Cognite equipment are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_equipment by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_equipment.delete("my_cognite_equipment")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_equipment.delete(my_ids)` please use `my_client.delete(my_ids)`."
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
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CogniteEquipment | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CogniteEquipmentList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CogniteEquipment | CogniteEquipmentList | None:
        """Retrieve one or more Cognite equipments by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite equipments.
            space: The space where all the Cognite equipments are located.
            retrieve_connections: Whether to retrieve `activities`, `asset`, `equipment_type`, `files`, `source` and
            `time_series` for the Cognite equipments. Defaults to 'skip'.'skip' will not retrieve any connections,
            'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full
            connected items.

        Returns:
            The requested Cognite equipments.

        Examples:

            Retrieve cognite_equipment by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_equipment = client.cognite_equipment.retrieve(
                ...     "my_cognite_equipment"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

    def search(
        self,
        query: str,
        properties: CogniteEquipmentTextFields | SequenceNotStr[CogniteEquipmentTextFields] | None = None,
        asset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_type: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        files: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        serial_number: str | list[str] | None = None,
        serial_number_prefix: str | None = None,
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
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteEquipmentFields | SequenceNotStr[CogniteEquipmentFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteEquipmentList:
        """Search Cognite equipments

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            asset: The asset to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment_type: The equipment type to filter on.
            files: The file to filter on.
            manufacturer: The manufacturer to filter on.
            manufacturer_prefix: The prefix of the manufacturer to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            serial_number: The serial number to filter on.
            serial_number_prefix: The prefix of the serial number to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite equipments to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite equipments matching the query.

        Examples:

           Search for 'my_cognite_equipment' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_equipments = client.cognite_equipment.search(
                ...     'my_cognite_equipment'
                ... )

        """
        filter_ = _create_cognite_equipment_filter(
            self._view_id,
            asset,
            description,
            description_prefix,
            equipment_type,
            files,
            manufacturer,
            manufacturer_prefix,
            name,
            name_prefix,
            serial_number,
            serial_number_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
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
        property: CogniteEquipmentFields | SequenceNotStr[CogniteEquipmentFields] | None = None,
        query: str | None = None,
        search_property: CogniteEquipmentTextFields | SequenceNotStr[CogniteEquipmentTextFields] | None = None,
        asset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_type: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        files: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        serial_number: str | list[str] | None = None,
        serial_number_prefix: str | None = None,
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
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
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
        property: CogniteEquipmentFields | SequenceNotStr[CogniteEquipmentFields] | None = None,
        query: str | None = None,
        search_property: CogniteEquipmentTextFields | SequenceNotStr[CogniteEquipmentTextFields] | None = None,
        asset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_type: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        files: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        serial_number: str | list[str] | None = None,
        serial_number_prefix: str | None = None,
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
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
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
        group_by: CogniteEquipmentFields | SequenceNotStr[CogniteEquipmentFields],
        property: CogniteEquipmentFields | SequenceNotStr[CogniteEquipmentFields] | None = None,
        query: str | None = None,
        search_property: CogniteEquipmentTextFields | SequenceNotStr[CogniteEquipmentTextFields] | None = None,
        asset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_type: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        files: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        serial_number: str | list[str] | None = None,
        serial_number_prefix: str | None = None,
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
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
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
        group_by: CogniteEquipmentFields | SequenceNotStr[CogniteEquipmentFields] | None = None,
        property: CogniteEquipmentFields | SequenceNotStr[CogniteEquipmentFields] | None = None,
        query: str | None = None,
        search_property: CogniteEquipmentTextFields | SequenceNotStr[CogniteEquipmentTextFields] | None = None,
        asset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_type: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        files: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        serial_number: str | list[str] | None = None,
        serial_number_prefix: str | None = None,
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
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite equipments

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            asset: The asset to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment_type: The equipment type to filter on.
            files: The file to filter on.
            manufacturer: The manufacturer to filter on.
            manufacturer_prefix: The prefix of the manufacturer to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            serial_number: The serial number to filter on.
            serial_number_prefix: The prefix of the serial number to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite equipments to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite equipments in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_equipment.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_equipment_filter(
            self._view_id,
            asset,
            description,
            description_prefix,
            equipment_type,
            files,
            manufacturer,
            manufacturer_prefix,
            name,
            name_prefix,
            serial_number,
            serial_number_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
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
        property: CogniteEquipmentFields,
        interval: float,
        query: str | None = None,
        search_property: CogniteEquipmentTextFields | SequenceNotStr[CogniteEquipmentTextFields] | None = None,
        asset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_type: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        files: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        serial_number: str | list[str] | None = None,
        serial_number_prefix: str | None = None,
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
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite equipments

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            asset: The asset to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment_type: The equipment type to filter on.
            files: The file to filter on.
            manufacturer: The manufacturer to filter on.
            manufacturer_prefix: The prefix of the manufacturer to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            serial_number: The serial number to filter on.
            serial_number_prefix: The prefix of the serial number to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite equipments to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_equipment_filter(
            self._view_id,
            asset,
            description,
            description_prefix,
            equipment_type,
            files,
            manufacturer,
            manufacturer_prefix,
            name,
            name_prefix,
            serial_number,
            serial_number_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
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

    def select(self) -> CogniteEquipmentQuery:
        """Start selecting from Cognite equipments."""
        return CogniteEquipmentQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> list[dict[str, Any]]:
        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=sort,
                limit=limit,
                has_container_fields=True,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_reverse_relation(
                    CogniteActivity._view_id,
                    through=dm.PropertyId(dm.ViewId("cdf_cdm", "CogniteActivity", "v1"), "equipment"),
                    connection_type="reverse-list",
                    connection_property=ViewPropertyId(self._view_id, "activities"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_reverse_relation(
                    CogniteTimeSeries._view_id,
                    through=dm.PropertyId(dm.ViewId("cdf_cdm", "CogniteTimeSeries", "v1"), "equipment"),
                    connection_type="reverse-list",
                    connection_property=ViewPropertyId(self._view_id, "timeSeries"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    CogniteAsset._view_id,
                    ViewPropertyId(self._view_id, "asset"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    CogniteEquipmentType._view_id,
                    ViewPropertyId(self._view_id, "equipmentType"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    CogniteFile._view_id,
                    ViewPropertyId(self._view_id, "files"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    CogniteSourceSystem._view_id,
                    ViewPropertyId(self._view_id, "source"),
                    has_container_fields=True,
                )
            )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        return QueryUnpacker(builder, edges=unpack_edges).unpack()

    def list(
        self,
        asset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_type: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        files: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        serial_number: str | list[str] | None = None,
        serial_number_prefix: str | None = None,
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
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteEquipmentFields | Sequence[CogniteEquipmentFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CogniteEquipmentList:
        """List/filter Cognite equipments

        Args:
            asset: The asset to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment_type: The equipment type to filter on.
            files: The file to filter on.
            manufacturer: The manufacturer to filter on.
            manufacturer_prefix: The prefix of the manufacturer to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            serial_number: The serial number to filter on.
            serial_number_prefix: The prefix of the serial number to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite equipments to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `activities`, `asset`, `equipment_type`, `files`, `source` and
            `time_series` for the Cognite equipments. Defaults to 'skip'.'skip' will not retrieve any connections,
            'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full
            connected items.

        Returns:
            List of requested Cognite equipments

        Examples:

            List Cognite equipments and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_equipments = client.cognite_equipment.list(limit=5)

        """
        filter_ = _create_cognite_equipment_filter(
            self._view_id,
            asset,
            description,
            description_prefix,
            equipment_type,
            files,
            manufacturer,
            manufacturer_prefix,
            name,
            name_prefix,
            serial_number,
            serial_number_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        values = self._query(filter_, limit, retrieve_connections, sort_input)
        return self._class_list(instantiate_classes(self._class_type, values, "list"))
