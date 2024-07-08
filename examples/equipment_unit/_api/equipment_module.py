from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from equipment_unit.data_classes._core import DEFAULT_INSTANCE_SPACE
from equipment_unit.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    EquipmentModule,
    EquipmentModuleWrite,
    EquipmentModuleFields,
    EquipmentModuleList,
    EquipmentModuleWriteList,
    EquipmentModuleTextFields,
)
from equipment_unit.data_classes._equipment_module import (
    _EQUIPMENTMODULE_PROPERTIES_BY_FIELD,
    _create_equipment_module_filter,
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
from .equipment_module_sensor_value import EquipmentModuleSensorValueAPI
from .equipment_module_query import EquipmentModuleQueryAPI


class EquipmentModuleAPI(NodeAPI[EquipmentModule, EquipmentModuleWrite, EquipmentModuleList, EquipmentModuleWriteList]):
    _view_id = dm.ViewId("IntegrationTestsImmutable", "EquipmentModule", "b1cd4bf14a7a33")
    _properties_by_field = _EQUIPMENTMODULE_PROPERTIES_BY_FIELD
    _class_type = EquipmentModule
    _class_list = EquipmentModuleList
    _class_write_list = EquipmentModuleWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.sensor_value = EquipmentModuleSensorValueAPI(client, self._view_id)

    def __call__(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> EquipmentModuleQueryAPI[EquipmentModuleList]:
        """Query starting at equipment modules.

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of equipment modules to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for equipment modules.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_equipment_module_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(EquipmentModuleList)
        return EquipmentModuleQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        equipment_module: EquipmentModuleWrite | Sequence[EquipmentModuleWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) equipment modules.

        Args:
            equipment_module: Equipment module or sequence of equipment modules to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new equipment_module:

                >>> from equipment_unit import EquipmentUnitClient
                >>> from equipment_unit.data_classes import EquipmentModuleWrite
                >>> client = EquipmentUnitClient()
                >>> equipment_module = EquipmentModuleWrite(external_id="my_equipment_module", ...)
                >>> result = client.equipment_module.apply(equipment_module)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.equipment_module.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(equipment_module, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more equipment module.

        Args:
            external_id: External id of the equipment module to delete.
            space: The space where all the equipment module are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete equipment_module by id:

                >>> from equipment_unit import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> client.equipment_module.delete("my_equipment_module")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.equipment_module.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> EquipmentModule | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> EquipmentModuleList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> EquipmentModule | EquipmentModuleList | None:
        """Retrieve one or more equipment modules by id(s).

        Args:
            external_id: External id or list of external ids of the equipment modules.
            space: The space where all the equipment modules are located.

        Returns:
            The requested equipment modules.

        Examples:

            Retrieve equipment_module by id:

                >>> from equipment_unit import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> equipment_module = client.equipment_module.retrieve("my_equipment_module")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: EquipmentModuleTextFields | SequenceNotStr[EquipmentModuleTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: EquipmentModuleFields | Sequence[EquipmentModuleFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> EquipmentModuleList:
        """Search equipment modules

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of equipment modules to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results equipment modules matching the query.

        Examples:

           Search for 'my_equipment_module' in all text properties:

                >>> from equipment_unit import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> equipment_modules = client.equipment_module.search('my_equipment_module')

        """
        filter_ = _create_equipment_module_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: EquipmentModuleFields | SequenceNotStr[EquipmentModuleFields] | None = None,
        query: str | None = None,
        search_properties: EquipmentModuleTextFields | SequenceNotStr[EquipmentModuleTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        property: EquipmentModuleFields | SequenceNotStr[EquipmentModuleFields] | None = None,
        query: str | None = None,
        search_properties: EquipmentModuleTextFields | SequenceNotStr[EquipmentModuleTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        group_by: EquipmentModuleFields | SequenceNotStr[EquipmentModuleFields],
        property: EquipmentModuleFields | SequenceNotStr[EquipmentModuleFields] | None = None,
        query: str | None = None,
        search_properties: EquipmentModuleTextFields | SequenceNotStr[EquipmentModuleTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        group_by: EquipmentModuleFields | SequenceNotStr[EquipmentModuleFields] | None = None,
        property: EquipmentModuleFields | SequenceNotStr[EquipmentModuleFields] | None = None,
        query: str | None = None,
        search_property: EquipmentModuleTextFields | SequenceNotStr[EquipmentModuleTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across equipment modules

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of equipment modules to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count equipment modules in space `my_space`:

                >>> from equipment_unit import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> result = client.equipment_module.aggregate("count", space="my_space")

        """

        filter_ = _create_equipment_module_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate,
            group_by,  # type: ignore[arg-type]
            property,  # type: ignore[arg-type]
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def histogram(
        self,
        property: EquipmentModuleFields,
        interval: float,
        query: str | None = None,
        search_property: EquipmentModuleTextFields | SequenceNotStr[EquipmentModuleTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for equipment modules

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of equipment modules to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_equipment_module_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            type_,
            type_prefix,
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

    def list(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: EquipmentModuleFields | Sequence[EquipmentModuleFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> EquipmentModuleList:
        """List/filter equipment modules

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of equipment modules to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested equipment modules

        Examples:

            List equipment modules and limit to 5:

                >>> from equipment_unit import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> equipment_modules = client.equipment_module.list(limit=5)

        """
        filter_ = _create_equipment_module_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            type_,
            type_prefix,
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
