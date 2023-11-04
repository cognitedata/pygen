from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    Meta,
    MetaApply,
    MetaList,
    MetaApplyList,
    MetaFields,
    MetaTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._meta import _META_PROPERTIES_BY_FIELD


class MetaAPI(TypeAPI[Meta, MetaApply, MetaList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[MetaApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Meta,
            class_apply_type=MetaApply,
            class_list=MetaList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(self, meta: MetaApply | Sequence[MetaApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) metas.

        Args:
            meta: Meta or sequence of metas to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new meta:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import MetaApply
                >>> client = OSDUClient()
                >>> meta = MetaApply(external_id="my_meta", ...)
                >>> result = client.meta.apply(meta)

        """
        if isinstance(meta, MetaApply):
            instances = meta.to_instances_apply(self._view_by_write_class)
        else:
            instances = MetaApplyList(meta).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more meta.

        Args:
            external_id: External id of the meta to delete.
            space: The space where all the meta are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete meta by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.meta.delete("my_meta")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Meta:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> MetaList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable") -> Meta | MetaList:
        """Retrieve one or more metas by id(s).

        Args:
            external_id: External id or list of external ids of the metas.
            space: The space where all the metas are located.

        Returns:
            The requested metas.

        Examples:

            Retrieve meta by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> meta = client.meta.retrieve("my_meta")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MetaList:
        """Search metas

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            persistable_reference: The persistable reference to filter on.
            persistable_reference_prefix: The prefix of the persistable reference to filter on.
            unit_of_measure_id: The unit of measure id to filter on.
            unit_of_measure_id_prefix: The prefix of the unit of measure id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results metas matching the query.

        Examples:

           Search for 'my_meta' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> metas = client.meta.search('my_meta')

        """
        filter_ = _create_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _META_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MetaFields | Sequence[MetaFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
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
        property: MetaFields | Sequence[MetaFields] | None = None,
        group_by: MetaFields | Sequence[MetaFields] = None,
        query: str | None = None,
        search_properties: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
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
        property: MetaFields | Sequence[MetaFields] | None = None,
        group_by: MetaFields | Sequence[MetaFields] | None = None,
        query: str | None = None,
        search_property: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _META_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MetaFields,
        interval: float,
        query: str | None = None,
        search_property: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _META_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MetaList:
        """List/filter metas

        Args:
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            persistable_reference: The persistable reference to filter on.
            persistable_reference_prefix: The prefix of the persistable reference to filter on.
            unit_of_measure_id: The unit of measure id to filter on.
            unit_of_measure_id_prefix: The prefix of the unit of measure id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested metas

        Examples:

            List metas and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> metas = client.meta.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    kind: str | list[str] | None = None,
    kind_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    persistable_reference: str | list[str] | None = None,
    persistable_reference_prefix: str | None = None,
    unit_of_measure_id: str | list[str] | None = None,
    unit_of_measure_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if kind and isinstance(kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("kind"), value=kind))
    if kind and isinstance(kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("kind"), values=kind))
    if kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("kind"), value=kind_prefix))
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if persistable_reference and isinstance(persistable_reference, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("persistableReference"), value=persistable_reference))
    if persistable_reference and isinstance(persistable_reference, list):
        filters.append(dm.filters.In(view_id.as_property_ref("persistableReference"), values=persistable_reference))
    if persistable_reference_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("persistableReference"), value=persistable_reference_prefix)
        )
    if unit_of_measure_id and isinstance(unit_of_measure_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("unitOfMeasureID"), value=unit_of_measure_id))
    if unit_of_measure_id and isinstance(unit_of_measure_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("unitOfMeasureID"), values=unit_of_measure_id))
    if unit_of_measure_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("unitOfMeasureID"), value=unit_of_measure_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
