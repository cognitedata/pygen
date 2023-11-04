from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    Acl,
    AclApply,
    AclList,
    AclApplyList,
    AclFields,
    AclTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._acl import _ACL_PROPERTIES_BY_FIELD


class AclAPI(TypeAPI[Acl, AclApply, AclList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AclApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Acl,
            class_apply_type=AclApply,
            class_list=AclList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(self, acl: AclApply | Sequence[AclApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) acls.

        Args:
            acl: Acl or sequence of acls to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new acl:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import AclApply
                >>> client = OSDUClient()
                >>> acl = AclApply(external_id="my_acl", ...)
                >>> result = client.acl.apply(acl)

        """
        if isinstance(acl, AclApply):
            instances = acl.to_instances_apply(self._view_by_write_class)
        else:
            instances = AclApplyList(acl).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more acl.

        Args:
            external_id: External id of the acl to delete.
            space: The space where all the acl are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete acl by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.acl.delete("my_acl")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Acl:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> AclList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable") -> Acl | AclList:
        """Retrieve one or more acls by id(s).

        Args:
            external_id: External id or list of external ids of the acls.
            space: The space where all the acls are located.

        Returns:
            The requested acls.

        Examples:

            Retrieve acl by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> acl = client.acl.retrieve("my_acl")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: AclTextFields | Sequence[AclTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AclList:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _ACL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AclFields | Sequence[AclFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AclTextFields | Sequence[AclTextFields] | None = None,
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
        property: AclFields | Sequence[AclFields] | None = None,
        group_by: AclFields | Sequence[AclFields] = None,
        query: str | None = None,
        search_properties: AclTextFields | Sequence[AclTextFields] | None = None,
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
        property: AclFields | Sequence[AclFields] | None = None,
        group_by: AclFields | Sequence[AclFields] | None = None,
        query: str | None = None,
        search_property: AclTextFields | Sequence[AclTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ACL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AclFields,
        interval: float,
        query: str | None = None,
        search_property: AclTextFields | Sequence[AclTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ACL_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AclList:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
