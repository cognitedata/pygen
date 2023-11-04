from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    LineageAssertions,
    LineageAssertionsApply,
    LineageAssertionsList,
    LineageAssertionsApplyList,
    LineageAssertionsFields,
    LineageAssertionsTextFields,
    DomainModelApply,
)
from osdu_wells_pydantic_v1.client.data_classes._lineage_assertions import _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD


class LineageAssertionsAPI(TypeAPI[LineageAssertions, LineageAssertionsApply, LineageAssertionsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[LineageAssertionsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=LineageAssertions,
            class_apply_type=LineageAssertionsApply,
            class_list=LineageAssertionsList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, lineage_assertion: LineageAssertionsApply | Sequence[LineageAssertionsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) lineage assertions.

        Args:
            lineage_assertion: Lineage assertion or sequence of lineage assertions to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new lineage_assertion:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import LineageAssertionsApply
                >>> client = OSDUClient()
                >>> lineage_assertion = LineageAssertionsApply(external_id="my_lineage_assertion", ...)
                >>> result = client.lineage_assertions.apply(lineage_assertion)

        """
        if isinstance(lineage_assertion, LineageAssertionsApply):
            instances = lineage_assertion.to_instances_apply(self._view_by_write_class)
        else:
            instances = LineageAssertionsApplyList(lineage_assertion).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more lineage assertion.

        Args:
            external_id: External id of the lineage assertion to delete.
            space: The space where all the lineage assertion are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete lineage_assertion by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.lineage_assertions.delete("my_lineage_assertion")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> LineageAssertions:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> LineageAssertionsList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> LineageAssertions | LineageAssertionsList:
        """Retrieve one or more lineage assertions by id(s).

        Args:
            external_id: External id or list of external ids of the lineage assertions.
            space: The space where all the lineage assertions are located.

        Returns:
            The requested lineage assertions.

        Examples:

            Retrieve lineage_assertion by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> lineage_assertion = client.lineage_assertions.retrieve("my_lineage_assertion")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LineageAssertionsList:
        """Search lineage assertions

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            id: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            lineage_relationship_type: The lineage relationship type to filter on.
            lineage_relationship_type_prefix: The prefix of the lineage relationship type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of lineage assertions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results lineage assertions matching the query.

        Examples:

           Search for 'my_lineage_assertion' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> lineage_assertions = client.lineage_assertions.search('my_lineage_assertion')

        """
        filter_ = _create_filter(
            self._view_id,
            id,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
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
        property: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        group_by: LineageAssertionsFields | Sequence[LineageAssertionsFields] = None,
        query: str | None = None,
        search_properties: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
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
        property: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        group_by: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        query: str | None = None,
        search_property: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across lineage assertions

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            id: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            lineage_relationship_type: The lineage relationship type to filter on.
            lineage_relationship_type_prefix: The prefix of the lineage relationship type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of lineage assertions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count lineage assertions in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.lineage_assertions.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            id,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: LineageAssertionsFields,
        interval: float,
        query: str | None = None,
        search_property: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for lineage assertions

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            id: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            lineage_relationship_type: The lineage relationship type to filter on.
            lineage_relationship_type_prefix: The prefix of the lineage relationship type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of lineage assertions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            id,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LineageAssertionsList:
        """List/filter lineage assertions

        Args:
            id: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            lineage_relationship_type: The lineage relationship type to filter on.
            lineage_relationship_type_prefix: The prefix of the lineage relationship type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of lineage assertions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested lineage assertions

        Examples:

            List lineage assertions and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> lineage_assertions = client.lineage_assertions.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            id,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    id: str | list[str] | None = None,
    id_prefix: str | None = None,
    lineage_relationship_type: str | list[str] | None = None,
    lineage_relationship_type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if id and isinstance(id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ID"), value=id))
    if id and isinstance(id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ID"), values=id))
    if id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ID"), value=id_prefix))
    if lineage_relationship_type and isinstance(lineage_relationship_type, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("LineageRelationshipType"), value=lineage_relationship_type)
        )
    if lineage_relationship_type and isinstance(lineage_relationship_type, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("LineageRelationshipType"), values=lineage_relationship_type)
        )
    if lineage_relationship_type_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("LineageRelationshipType"), value=lineage_relationship_type_prefix
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
