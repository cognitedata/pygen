from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    Artefacts,
    ArtefactsApply,
    ArtefactsList,
    ArtefactsApplyList,
    ArtefactsFields,
    ArtefactsTextFields,
    DomainModelApply,
)
from osdu_wells_pydantic_v1.client.data_classes._artefacts import _ARTEFACTS_PROPERTIES_BY_FIELD


class ArtefactsAPI(TypeAPI[Artefacts, ArtefactsApply, ArtefactsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[ArtefactsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Artefacts,
            class_apply_type=ArtefactsApply,
            class_list=ArtefactsList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, artefact: ArtefactsApply | Sequence[ArtefactsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) artefacts.

        Args:
            artefact: Artefact or sequence of artefacts to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new artefact:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import ArtefactsApply
                >>> client = OSDUClient()
                >>> artefact = ArtefactsApply(external_id="my_artefact", ...)
                >>> result = client.artefacts.apply(artefact)

        """
        if isinstance(artefact, ArtefactsApply):
            instances = artefact.to_instances_apply(self._view_by_write_class)
        else:
            instances = ArtefactsApplyList(artefact).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more artefact.

        Args:
            external_id: External id of the artefact to delete.
            space: The space where all the artefact are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete artefact by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.artefacts.delete("my_artefact")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Artefacts:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ArtefactsList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> Artefacts | ArtefactsList:
        """Retrieve one or more artefacts by id(s).

        Args:
            external_id: External id or list of external ids of the artefacts.
            space: The space where all the artefacts are located.

        Returns:
            The requested artefacts.

        Examples:

            Retrieve artefact by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> artefact = client.artefacts.retrieve("my_artefact")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: ArtefactsTextFields | Sequence[ArtefactsTextFields] | None = None,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ArtefactsList:
        """Search artefacts

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            resource_id: The resource id to filter on.
            resource_id_prefix: The prefix of the resource id to filter on.
            resource_kind: The resource kind to filter on.
            resource_kind_prefix: The prefix of the resource kind to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of artefacts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results artefacts matching the query.

        Examples:

           Search for 'my_artefact' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> artefacts = client.artefacts.search('my_artefact')

        """
        filter_ = _create_filter(
            self._view_id,
            resource_id,
            resource_id_prefix,
            resource_kind,
            resource_kind_prefix,
            role_id,
            role_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _ARTEFACTS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ArtefactsFields | Sequence[ArtefactsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ArtefactsTextFields | Sequence[ArtefactsTextFields] | None = None,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
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
        property: ArtefactsFields | Sequence[ArtefactsFields] | None = None,
        group_by: ArtefactsFields | Sequence[ArtefactsFields] = None,
        query: str | None = None,
        search_properties: ArtefactsTextFields | Sequence[ArtefactsTextFields] | None = None,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
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
        property: ArtefactsFields | Sequence[ArtefactsFields] | None = None,
        group_by: ArtefactsFields | Sequence[ArtefactsFields] | None = None,
        query: str | None = None,
        search_property: ArtefactsTextFields | Sequence[ArtefactsTextFields] | None = None,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across artefacts

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            resource_id: The resource id to filter on.
            resource_id_prefix: The prefix of the resource id to filter on.
            resource_kind: The resource kind to filter on.
            resource_kind_prefix: The prefix of the resource kind to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of artefacts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count artefacts in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.artefacts.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            resource_id,
            resource_id_prefix,
            resource_kind,
            resource_kind_prefix,
            role_id,
            role_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ARTEFACTS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ArtefactsFields,
        interval: float,
        query: str | None = None,
        search_property: ArtefactsTextFields | Sequence[ArtefactsTextFields] | None = None,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for artefacts

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            resource_id: The resource id to filter on.
            resource_id_prefix: The prefix of the resource id to filter on.
            resource_kind: The resource kind to filter on.
            resource_kind_prefix: The prefix of the resource kind to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of artefacts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            resource_id,
            resource_id_prefix,
            resource_kind,
            resource_kind_prefix,
            role_id,
            role_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ARTEFACTS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ArtefactsList:
        """List/filter artefacts

        Args:
            resource_id: The resource id to filter on.
            resource_id_prefix: The prefix of the resource id to filter on.
            resource_kind: The resource kind to filter on.
            resource_kind_prefix: The prefix of the resource kind to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of artefacts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested artefacts

        Examples:

            List artefacts and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> artefacts = client.artefacts.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            resource_id,
            resource_id_prefix,
            resource_kind,
            resource_kind_prefix,
            role_id,
            role_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    resource_id: str | list[str] | None = None,
    resource_id_prefix: str | None = None,
    resource_kind: str | list[str] | None = None,
    resource_kind_prefix: str | None = None,
    role_id: str | list[str] | None = None,
    role_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if resource_id and isinstance(resource_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ResourceID"), value=resource_id))
    if resource_id and isinstance(resource_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ResourceID"), values=resource_id))
    if resource_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ResourceID"), value=resource_id_prefix))
    if resource_kind and isinstance(resource_kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ResourceKind"), value=resource_kind))
    if resource_kind and isinstance(resource_kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ResourceKind"), values=resource_kind))
    if resource_kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ResourceKind"), value=resource_kind_prefix))
    if role_id and isinstance(role_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("RoleID"), value=role_id))
    if role_id and isinstance(role_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("RoleID"), values=role_id))
    if role_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("RoleID"), value=role_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
