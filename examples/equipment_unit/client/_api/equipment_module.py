from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from equipment_unit.client.data_classes import (
    ResourcesApplyResult,
    EquipmentModule,
    EquipmentModuleApply,
    EquipmentModuleFields,
    EquipmentModuleList,
    EquipmentModuleTextFields,
    DomainModelApply,
)
from equipment_unit.client.data_classes._equipment_module import (
    _EQUIPMENTMODULE_PROPERTIES_BY_FIELD,
    _create_equipment_module_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, QueryStep, QueryBuilder
from .equipment_module_sensor_value import EquipmentModuleSensorValueAPI
from .equipment_module_query import EquipmentModuleQueryAPI


class EquipmentModuleAPI(NodeAPI[EquipmentModule, EquipmentModuleApply, EquipmentModuleList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[EquipmentModuleApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=EquipmentModule,
            class_apply_type=EquipmentModuleApply,
            class_list=EquipmentModuleList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.sensor_value = EquipmentModuleSensorValueAPI(client, view_id)

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
        limit: int = DEFAULT_LIMIT_READ,
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
        builder = QueryBuilder(
            EquipmentModuleList,
            [
                QueryStep(
                    name="equipment_module",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_EQUIPMENTMODULE_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=EquipmentModule,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return EquipmentModuleQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, equipment_module: EquipmentModuleApply | Sequence[EquipmentModuleApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) equipment modules.

        Args:
            equipment_module: Equipment module or sequence of equipment modules to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new equipment_module:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> from equipment_unit.client.data_classes import EquipmentModuleApply
                >>> client = EquipmentUnitClient()
                >>> equipment_module = EquipmentModuleApply(external_id="my_equipment_module", ...)
                >>> result = client.equipment_module.apply(equipment_module)

        """
        return self._apply(equipment_module, replace)

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more equipment module.

        Args:
            external_id: External id of the equipment module to delete.
            space: The space where all the equipment module are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete equipment_module by id:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> client.equipment_module.delete("my_equipment_module")
        """
        return self._delete(external_id, space=space)

    @overload
    def retrieve(self, external_id: str) -> EquipmentModule:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> EquipmentModuleList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> EquipmentModule | EquipmentModuleList:
        """Retrieve one or more equipment modules by id(s).

        Args:
            external_id: External id or list of external ids of the equipment modules.
            space: The space where all the equipment modules are located.

        Returns:
            The requested equipment modules.

        Examples:

            Retrieve equipment_module by id:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> equipment_module = client.equipment_module.retrieve("my_equipment_module")

        """
        return self._retrieve(external_id, space=space)

    def search(
        self,
        query: str,
        properties: EquipmentModuleTextFields | Sequence[EquipmentModuleTextFields] | None = None,
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

        Returns:
            Search results equipment modules matching the query.

        Examples:

           Search for 'my_equipment_module' in all text properties:

                >>> from equipment_unit.client import EquipmentUnitClient
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
        return self._search(self._view_id, query, _EQUIPMENTMODULE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: EquipmentModuleFields | Sequence[EquipmentModuleFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: EquipmentModuleTextFields | Sequence[EquipmentModuleTextFields] | None = None,
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
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: EquipmentModuleFields | Sequence[EquipmentModuleFields] | None = None,
        group_by: EquipmentModuleFields | Sequence[EquipmentModuleFields] = None,
        query: str | None = None,
        search_properties: EquipmentModuleTextFields | Sequence[EquipmentModuleTextFields] | None = None,
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
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: EquipmentModuleFields | Sequence[EquipmentModuleFields] | None = None,
        group_by: EquipmentModuleFields | Sequence[EquipmentModuleFields] | None = None,
        query: str | None = None,
        search_property: EquipmentModuleTextFields | Sequence[EquipmentModuleTextFields] | None = None,
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
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across equipment modules

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
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

                >>> from equipment_unit.client import EquipmentUnitClient
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
            self._view_id,
            aggregate,
            _EQUIPMENTMODULE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: EquipmentModuleFields,
        interval: float,
        query: str | None = None,
        search_property: EquipmentModuleTextFields | Sequence[EquipmentModuleTextFields] | None = None,
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
            self._view_id,
            property,
            interval,
            _EQUIPMENTMODULE_PROPERTIES_BY_FIELD,
            query,
            search_property,
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

        Returns:
            List of requested equipment modules

        Examples:

            List equipment modules and limit to 5:

                >>> from equipment_unit.client import EquipmentUnitClient
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

        return self._list(limit=limit, filter=filter_)
