from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    Legal,
    LegalApply,
    LegalList,
    LegalApplyList,
    LegalFields,
    LegalTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._legal import _LEGAL_PROPERTIES_BY_FIELD


class LegalAPI(TypeAPI[Legal, LegalApply, LegalList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[LegalApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Legal,
            class_apply_type=LegalApply,
            class_list=LegalList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(self, legal: LegalApply | Sequence[LegalApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) legals.

        Args:
            legal: Legal or sequence of legals to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new legal:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import LegalApply
                >>> client = OSDUClient()
                >>> legal = LegalApply(external_id="my_legal", ...)
                >>> result = client.legal.apply(legal)

        """
        if isinstance(legal, LegalApply):
            instances = legal.to_instances_apply(self._view_by_write_class)
        else:
            instances = LegalApplyList(legal).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more legal.

        Args:
            external_id: External id of the legal to delete.
            space: The space where all the legal are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete legal by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.legal.delete("my_legal")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Legal:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> LegalList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable") -> Legal | LegalList:
        """Retrieve one or more legals by id(s).

        Args:
            external_id: External id or list of external ids of the legals.
            space: The space where all the legals are located.

        Returns:
            The requested legals.

        Examples:

            Retrieve legal by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> legal = client.legal.retrieve("my_legal")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LegalList:
        """Search legals

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of legals to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results legals matching the query.

        Examples:

           Search for 'my_legal' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> legals = client.legal.search('my_legal')

        """
        filter_ = _create_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _LEGAL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: LegalFields | Sequence[LegalFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
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
        property: LegalFields | Sequence[LegalFields] | None = None,
        group_by: LegalFields | Sequence[LegalFields] = None,
        query: str | None = None,
        search_properties: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
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
        property: LegalFields | Sequence[LegalFields] | None = None,
        group_by: LegalFields | Sequence[LegalFields] | None = None,
        query: str | None = None,
        search_property: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _LEGAL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: LegalFields,
        interval: float,
        query: str | None = None,
        search_property: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _LEGAL_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LegalList:
        """List/filter legals

        Args:
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of legals to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested legals

        Examples:

            List legals and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> legals = client.legal.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    status: str | list[str] | None = None,
    status_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if status and isinstance(status, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("status"), value=status))
    if status and isinstance(status, list):
        filters.append(dm.filters.In(view_id.as_property_ref("status"), values=status))
    if status_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("status"), value=status_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
