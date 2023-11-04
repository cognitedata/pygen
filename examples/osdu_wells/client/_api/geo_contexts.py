from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    GeoContexts,
    GeoContextsApply,
    GeoContextsList,
    GeoContextsApplyList,
    GeoContextsFields,
    GeoContextsTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._geo_contexts import _GEOCONTEXTS_PROPERTIES_BY_FIELD


class GeoContextsAPI(TypeAPI[GeoContexts, GeoContextsApply, GeoContextsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[GeoContextsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=GeoContexts,
            class_apply_type=GeoContextsApply,
            class_list=GeoContextsList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, geo_context: GeoContextsApply | Sequence[GeoContextsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) geo contexts.

        Args:
            geo_context: Geo context or sequence of geo contexts to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new geo_context:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import GeoContextsApply
                >>> client = OSDUClient()
                >>> geo_context = GeoContextsApply(external_id="my_geo_context", ...)
                >>> result = client.geo_contexts.apply(geo_context)

        """
        if isinstance(geo_context, GeoContextsApply):
            instances = geo_context.to_instances_apply(self._view_by_write_class)
        else:
            instances = GeoContextsApplyList(geo_context).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more geo context.

        Args:
            external_id: External id of the geo context to delete.
            space: The space where all the geo context are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete geo_context by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.geo_contexts.delete("my_geo_context")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> GeoContexts:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> GeoContextsList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> GeoContexts | GeoContextsList:
        """Retrieve one or more geo contexts by id(s).

        Args:
            external_id: External id or list of external ids of the geo contexts.
            space: The space where all the geo contexts are located.

        Returns:
            The requested geo contexts.

        Examples:

            Retrieve geo_context by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> geo_context = client.geo_contexts.retrieve("my_geo_context")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: GeoContextsTextFields | Sequence[GeoContextsTextFields] | None = None,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> GeoContextsList:
        """Search geo contexts

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            basin_id: The basin id to filter on.
            basin_id_prefix: The prefix of the basin id to filter on.
            field_id: The field id to filter on.
            field_id_prefix: The prefix of the field id to filter on.
            geo_political_entity_id: The geo political entity id to filter on.
            geo_political_entity_id_prefix: The prefix of the geo political entity id to filter on.
            geo_type_id: The geo type id to filter on.
            geo_type_id_prefix: The prefix of the geo type id to filter on.
            play_id: The play id to filter on.
            play_id_prefix: The prefix of the play id to filter on.
            prospect_id: The prospect id to filter on.
            prospect_id_prefix: The prefix of the prospect id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of geo contexts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results geo contexts matching the query.

        Examples:

           Search for 'my_geo_context' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> geo_contexts = client.geo_contexts.search('my_geo_context')

        """
        filter_ = _create_filter(
            self._view_id,
            basin_id,
            basin_id_prefix,
            field_id,
            field_id_prefix,
            geo_political_entity_id,
            geo_political_entity_id_prefix,
            geo_type_id,
            geo_type_id_prefix,
            play_id,
            play_id_prefix,
            prospect_id,
            prospect_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _GEOCONTEXTS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: GeoContextsFields | Sequence[GeoContextsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: GeoContextsTextFields | Sequence[GeoContextsTextFields] | None = None,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
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
        property: GeoContextsFields | Sequence[GeoContextsFields] | None = None,
        group_by: GeoContextsFields | Sequence[GeoContextsFields] = None,
        query: str | None = None,
        search_properties: GeoContextsTextFields | Sequence[GeoContextsTextFields] | None = None,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
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
        property: GeoContextsFields | Sequence[GeoContextsFields] | None = None,
        group_by: GeoContextsFields | Sequence[GeoContextsFields] | None = None,
        query: str | None = None,
        search_property: GeoContextsTextFields | Sequence[GeoContextsTextFields] | None = None,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across geo contexts

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            basin_id: The basin id to filter on.
            basin_id_prefix: The prefix of the basin id to filter on.
            field_id: The field id to filter on.
            field_id_prefix: The prefix of the field id to filter on.
            geo_political_entity_id: The geo political entity id to filter on.
            geo_political_entity_id_prefix: The prefix of the geo political entity id to filter on.
            geo_type_id: The geo type id to filter on.
            geo_type_id_prefix: The prefix of the geo type id to filter on.
            play_id: The play id to filter on.
            play_id_prefix: The prefix of the play id to filter on.
            prospect_id: The prospect id to filter on.
            prospect_id_prefix: The prefix of the prospect id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of geo contexts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count geo contexts in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.geo_contexts.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            basin_id,
            basin_id_prefix,
            field_id,
            field_id_prefix,
            geo_political_entity_id,
            geo_political_entity_id_prefix,
            geo_type_id,
            geo_type_id_prefix,
            play_id,
            play_id_prefix,
            prospect_id,
            prospect_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _GEOCONTEXTS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: GeoContextsFields,
        interval: float,
        query: str | None = None,
        search_property: GeoContextsTextFields | Sequence[GeoContextsTextFields] | None = None,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for geo contexts

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            basin_id: The basin id to filter on.
            basin_id_prefix: The prefix of the basin id to filter on.
            field_id: The field id to filter on.
            field_id_prefix: The prefix of the field id to filter on.
            geo_political_entity_id: The geo political entity id to filter on.
            geo_political_entity_id_prefix: The prefix of the geo political entity id to filter on.
            geo_type_id: The geo type id to filter on.
            geo_type_id_prefix: The prefix of the geo type id to filter on.
            play_id: The play id to filter on.
            play_id_prefix: The prefix of the play id to filter on.
            prospect_id: The prospect id to filter on.
            prospect_id_prefix: The prefix of the prospect id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of geo contexts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            basin_id,
            basin_id_prefix,
            field_id,
            field_id_prefix,
            geo_political_entity_id,
            geo_political_entity_id_prefix,
            geo_type_id,
            geo_type_id_prefix,
            play_id,
            play_id_prefix,
            prospect_id,
            prospect_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _GEOCONTEXTS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> GeoContextsList:
        """List/filter geo contexts

        Args:
            basin_id: The basin id to filter on.
            basin_id_prefix: The prefix of the basin id to filter on.
            field_id: The field id to filter on.
            field_id_prefix: The prefix of the field id to filter on.
            geo_political_entity_id: The geo political entity id to filter on.
            geo_political_entity_id_prefix: The prefix of the geo political entity id to filter on.
            geo_type_id: The geo type id to filter on.
            geo_type_id_prefix: The prefix of the geo type id to filter on.
            play_id: The play id to filter on.
            play_id_prefix: The prefix of the play id to filter on.
            prospect_id: The prospect id to filter on.
            prospect_id_prefix: The prefix of the prospect id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of geo contexts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested geo contexts

        Examples:

            List geo contexts and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> geo_contexts = client.geo_contexts.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            basin_id,
            basin_id_prefix,
            field_id,
            field_id_prefix,
            geo_political_entity_id,
            geo_political_entity_id_prefix,
            geo_type_id,
            geo_type_id_prefix,
            play_id,
            play_id_prefix,
            prospect_id,
            prospect_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    basin_id: str | list[str] | None = None,
    basin_id_prefix: str | None = None,
    field_id: str | list[str] | None = None,
    field_id_prefix: str | None = None,
    geo_political_entity_id: str | list[str] | None = None,
    geo_political_entity_id_prefix: str | None = None,
    geo_type_id: str | list[str] | None = None,
    geo_type_id_prefix: str | None = None,
    play_id: str | list[str] | None = None,
    play_id_prefix: str | None = None,
    prospect_id: str | list[str] | None = None,
    prospect_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if basin_id and isinstance(basin_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("BasinID"), value=basin_id))
    if basin_id and isinstance(basin_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("BasinID"), values=basin_id))
    if basin_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("BasinID"), value=basin_id_prefix))
    if field_id and isinstance(field_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FieldID"), value=field_id))
    if field_id and isinstance(field_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FieldID"), values=field_id))
    if field_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("FieldID"), value=field_id_prefix))
    if geo_political_entity_id and isinstance(geo_political_entity_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("GeoPoliticalEntityID"), value=geo_political_entity_id)
        )
    if geo_political_entity_id and isinstance(geo_political_entity_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("GeoPoliticalEntityID"), values=geo_political_entity_id))
    if geo_political_entity_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("GeoPoliticalEntityID"), value=geo_political_entity_id_prefix)
        )
    if geo_type_id and isinstance(geo_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("GeoTypeID"), value=geo_type_id))
    if geo_type_id and isinstance(geo_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("GeoTypeID"), values=geo_type_id))
    if geo_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("GeoTypeID"), value=geo_type_id_prefix))
    if play_id and isinstance(play_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("PlayID"), value=play_id))
    if play_id and isinstance(play_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("PlayID"), values=play_id))
    if play_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("PlayID"), value=play_id_prefix))
    if prospect_id and isinstance(prospect_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ProspectID"), value=prospect_id))
    if prospect_id and isinstance(prospect_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ProspectID"), values=prospect_id))
    if prospect_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ProspectID"), value=prospect_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
