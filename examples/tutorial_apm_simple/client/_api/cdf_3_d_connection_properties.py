from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from tutorial_apm_simple.client.data_classes import (
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
    CdfConnectionPropertiesList,
    CdfConnectionPropertiesApplyList,
    CdfConnectionPropertiesFields,
    DomainModelApply,
)
from tutorial_apm_simple.client.data_classes._cdf_3_d_connection_properties import (
    _CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD,
)


class CdfConnectionPropertiesAPI(
    TypeAPI[CdfConnectionProperties, CdfConnectionPropertiesApply, CdfConnectionPropertiesList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CdfConnectionPropertiesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CdfConnectionProperties,
            class_apply_type=CdfConnectionPropertiesApply,
            class_list=CdfConnectionPropertiesList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self,
        cdf_3_d_connection_property: CdfConnectionPropertiesApply | Sequence[CdfConnectionPropertiesApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) cdf 3 d connection properties.

        Args:
            cdf_3_d_connection_property: Cdf 3 d connection property or sequence of cdf 3 d connection properties to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new cdf_3_d_connection_property:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> from tutorial_apm_simple.client.data_classes import CdfConnectionPropertiesApply
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_connection_property = CdfConnectionPropertiesApply(external_id="my_cdf_3_d_connection_property", ...)
                >>> result = client.cdf_3_d_connection_properties.apply(cdf_3_d_connection_property)

        """
        if isinstance(cdf_3_d_connection_property, CdfConnectionPropertiesApply):
            instances = cdf_3_d_connection_property.to_instances_apply(self._view_by_write_class)
        else:
            instances = CdfConnectionPropertiesApplyList(cdf_3_d_connection_property).to_instances_apply(
                self._view_by_write_class
            )
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space: str = "cdf_3d_schema") -> dm.InstancesDeleteResult:
        """Delete one or more cdf 3 d connection property.

        Args:
            external_id: External id of the cdf 3 d connection property to delete.
            space: The space where all the cdf 3 d connection property are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cdf_3_d_connection_property by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> client.cdf_3_d_connection_properties.delete("my_cdf_3_d_connection_property")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CdfConnectionProperties:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CdfConnectionPropertiesList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "cdf_3d_schema"
    ) -> CdfConnectionProperties | CdfConnectionPropertiesList:
        """Retrieve one or more cdf 3 d connection properties by id(s).

        Args:
            external_id: External id or list of external ids of the cdf 3 d connection properties.
            space: The space where all the cdf 3 d connection properties are located.

        Returns:
            The requested cdf 3 d connection properties.

        Examples:

            Retrieve cdf_3_d_connection_property by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_connection_property = client.cdf_3_d_connection_properties.retrieve("my_cdf_3_d_connection_property")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CdfConnectionPropertiesFields | Sequence[CdfConnectionPropertiesFields] | None = None,
        group_by: None = None,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
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
        property: CdfConnectionPropertiesFields | Sequence[CdfConnectionPropertiesFields] | None = None,
        group_by: CdfConnectionPropertiesFields | Sequence[CdfConnectionPropertiesFields] = None,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
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
        property: CdfConnectionPropertiesFields | Sequence[CdfConnectionPropertiesFields] | None = None,
        group_by: CdfConnectionPropertiesFields | Sequence[CdfConnectionPropertiesFields] | None = None,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            min_revision_id,
            max_revision_id,
            min_revision_node_id,
            max_revision_node_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CdfConnectionPropertiesFields,
        interval: float,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            min_revision_id,
            max_revision_id,
            min_revision_node_id,
            max_revision_node_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CdfConnectionPropertiesList:
        filter_ = _create_filter(
            self._view_id,
            min_revision_id,
            max_revision_id,
            min_revision_node_id,
            max_revision_node_id,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_revision_id: int | None = None,
    max_revision_id: int | None = None,
    min_revision_node_id: int | None = None,
    max_revision_node_id: int | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_revision_id or max_revision_id:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("revisionId"), gte=min_revision_id, lte=max_revision_id)
        )
    if min_revision_node_id or max_revision_node_id:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("revisionNodeId"), gte=min_revision_node_id, lte=max_revision_node_id
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
