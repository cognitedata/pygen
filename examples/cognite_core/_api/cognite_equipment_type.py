from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
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
from cognite_core.data_classes._cognite_equipment_type import (
    CogniteEquipmentTypeQuery,
    _COGNITEEQUIPMENTTYPE_PROPERTIES_BY_FIELD,
    _create_cognite_equipment_type_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CogniteEquipmentType,
    CogniteEquipmentTypeWrite,
    CogniteEquipmentTypeFields,
    CogniteEquipmentTypeList,
    CogniteEquipmentTypeWriteList,
    CogniteEquipmentTypeTextFields,
)
from cognite_core._api.cognite_equipment_type_query import CogniteEquipmentTypeQueryAPI


class CogniteEquipmentTypeAPI(
    NodeAPI[CogniteEquipmentType, CogniteEquipmentTypeWrite, CogniteEquipmentTypeList, CogniteEquipmentTypeWriteList]
):
    _view_id = dm.ViewId("cdf_cdm", "CogniteEquipmentType", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITEEQUIPMENTTYPE_PROPERTIES_BY_FIELD
    _class_type = CogniteEquipmentType
    _class_list = CogniteEquipmentTypeList
    _class_write_list = CogniteEquipmentTypeWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_class: str | list[str] | None = None,
        equipment_class_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogniteEquipmentTypeQueryAPI[CogniteEquipmentType, CogniteEquipmentTypeList]:
        """Query starting at Cognite equipment types.

        Args:
            code: The code to filter on.
            code_prefix: The prefix of the code to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment_class: The equipment clas to filter on.
            equipment_class_prefix: The prefix of the equipment clas to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            standard: The standard to filter on.
            standard_prefix: The prefix of the standard to filter on.
            standard_reference: The standard reference to filter on.
            standard_reference_prefix: The prefix of the standard reference to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite equipment types to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite equipment types.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_equipment_type_filter(
            self._view_id,
            code,
            code_prefix,
            description,
            description_prefix,
            equipment_class,
            equipment_class_prefix,
            name,
            name_prefix,
            standard,
            standard_prefix,
            standard_reference,
            standard_reference_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return ConnectionItemAQueryAPI(self._client, QueryBuilder(), self._class_type, self._class_list, filter_, limit)

    def apply(
        self,
        cognite_equipment_type: CogniteEquipmentTypeWrite | Sequence[CogniteEquipmentTypeWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite equipment types.

        Args:
            cognite_equipment_type: Cognite equipment type or
                sequence of Cognite equipment types to upsert.
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

            Create a new cognite_equipment_type:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CogniteEquipmentTypeWrite
                >>> client = CogniteCoreClient()
                >>> cognite_equipment_type = CogniteEquipmentTypeWrite(
                ...     external_id="my_cognite_equipment_type", ...
                ... )
                >>> result = client.cognite_equipment_type.apply(cognite_equipment_type)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_equipment_type.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_equipment_type, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite equipment type.

        Args:
            external_id: External id of the Cognite equipment type to delete.
            space: The space where all the Cognite equipment type are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_equipment_type by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_equipment_type.delete("my_cognite_equipment_type")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_equipment_type.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CogniteEquipmentType | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CogniteEquipmentTypeList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteEquipmentType | CogniteEquipmentTypeList | None:
        """Retrieve one or more Cognite equipment types by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite equipment types.
            space: The space where all the Cognite equipment types are located.

        Returns:
            The requested Cognite equipment types.

        Examples:

            Retrieve cognite_equipment_type by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_equipment_type = client.cognite_equipment_type.retrieve(
                ...     "my_cognite_equipment_type"
                ... )

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: CogniteEquipmentTypeTextFields | SequenceNotStr[CogniteEquipmentTypeTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_class: str | list[str] | None = None,
        equipment_class_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteEquipmentTypeFields | SequenceNotStr[CogniteEquipmentTypeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteEquipmentTypeList:
        """Search Cognite equipment types

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            code: The code to filter on.
            code_prefix: The prefix of the code to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment_class: The equipment clas to filter on.
            equipment_class_prefix: The prefix of the equipment clas to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            standard: The standard to filter on.
            standard_prefix: The prefix of the standard to filter on.
            standard_reference: The standard reference to filter on.
            standard_reference_prefix: The prefix of the standard reference to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite equipment types to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite equipment types matching the query.

        Examples:

           Search for 'my_cognite_equipment_type' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_equipment_types = client.cognite_equipment_type.search(
                ...     'my_cognite_equipment_type'
                ... )

        """
        filter_ = _create_cognite_equipment_type_filter(
            self._view_id,
            code,
            code_prefix,
            description,
            description_prefix,
            equipment_class,
            equipment_class_prefix,
            name,
            name_prefix,
            standard,
            standard_prefix,
            standard_reference,
            standard_reference_prefix,
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
        property: CogniteEquipmentTypeFields | SequenceNotStr[CogniteEquipmentTypeFields] | None = None,
        query: str | None = None,
        search_property: CogniteEquipmentTypeTextFields | SequenceNotStr[CogniteEquipmentTypeTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_class: str | list[str] | None = None,
        equipment_class_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
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
        property: CogniteEquipmentTypeFields | SequenceNotStr[CogniteEquipmentTypeFields] | None = None,
        query: str | None = None,
        search_property: CogniteEquipmentTypeTextFields | SequenceNotStr[CogniteEquipmentTypeTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_class: str | list[str] | None = None,
        equipment_class_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
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
        group_by: CogniteEquipmentTypeFields | SequenceNotStr[CogniteEquipmentTypeFields],
        property: CogniteEquipmentTypeFields | SequenceNotStr[CogniteEquipmentTypeFields] | None = None,
        query: str | None = None,
        search_property: CogniteEquipmentTypeTextFields | SequenceNotStr[CogniteEquipmentTypeTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_class: str | list[str] | None = None,
        equipment_class_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
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
        group_by: CogniteEquipmentTypeFields | SequenceNotStr[CogniteEquipmentTypeFields] | None = None,
        property: CogniteEquipmentTypeFields | SequenceNotStr[CogniteEquipmentTypeFields] | None = None,
        query: str | None = None,
        search_property: CogniteEquipmentTypeTextFields | SequenceNotStr[CogniteEquipmentTypeTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_class: str | list[str] | None = None,
        equipment_class_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite equipment types

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            code: The code to filter on.
            code_prefix: The prefix of the code to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment_class: The equipment clas to filter on.
            equipment_class_prefix: The prefix of the equipment clas to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            standard: The standard to filter on.
            standard_prefix: The prefix of the standard to filter on.
            standard_reference: The standard reference to filter on.
            standard_reference_prefix: The prefix of the standard reference to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite equipment types to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite equipment types in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_equipment_type.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_equipment_type_filter(
            self._view_id,
            code,
            code_prefix,
            description,
            description_prefix,
            equipment_class,
            equipment_class_prefix,
            name,
            name_prefix,
            standard,
            standard_prefix,
            standard_reference,
            standard_reference_prefix,
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
        property: CogniteEquipmentTypeFields,
        interval: float,
        query: str | None = None,
        search_property: CogniteEquipmentTypeTextFields | SequenceNotStr[CogniteEquipmentTypeTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_class: str | list[str] | None = None,
        equipment_class_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite equipment types

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            code: The code to filter on.
            code_prefix: The prefix of the code to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment_class: The equipment clas to filter on.
            equipment_class_prefix: The prefix of the equipment clas to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            standard: The standard to filter on.
            standard_prefix: The prefix of the standard to filter on.
            standard_reference: The standard reference to filter on.
            standard_reference_prefix: The prefix of the standard reference to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite equipment types to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_equipment_type_filter(
            self._view_id,
            code,
            code_prefix,
            description,
            description_prefix,
            equipment_class,
            equipment_class_prefix,
            name,
            name_prefix,
            standard,
            standard_prefix,
            standard_reference,
            standard_reference_prefix,
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

    def query(self) -> CogniteEquipmentTypeQuery:
        """Start a query for Cognite equipment types."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return CogniteEquipmentTypeQuery(self._client)

    def select(self) -> CogniteEquipmentTypeQuery:
        """Start selecting from Cognite equipment types."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return CogniteEquipmentTypeQuery(self._client)

    def list(
        self,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        equipment_class: str | list[str] | None = None,
        equipment_class_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteEquipmentTypeFields | Sequence[CogniteEquipmentTypeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteEquipmentTypeList:
        """List/filter Cognite equipment types

        Args:
            code: The code to filter on.
            code_prefix: The prefix of the code to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            equipment_class: The equipment clas to filter on.
            equipment_class_prefix: The prefix of the equipment clas to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            standard: The standard to filter on.
            standard_prefix: The prefix of the standard to filter on.
            standard_reference: The standard reference to filter on.
            standard_reference_prefix: The prefix of the standard reference to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite equipment types to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested Cognite equipment types

        Examples:

            List Cognite equipment types and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_equipment_types = client.cognite_equipment_type.list(limit=5)

        """
        filter_ = _create_cognite_equipment_type_filter(
            self._view_id,
            code,
            code_prefix,
            description,
            description_prefix,
            equipment_class,
            equipment_class_prefix,
            name,
            name_prefix,
            standard,
            standard_prefix,
            standard_reference,
            standard_reference_prefix,
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
