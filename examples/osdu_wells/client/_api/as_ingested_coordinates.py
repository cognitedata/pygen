from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    AsIngestedCoordinates,
    AsIngestedCoordinatesApply,
    AsIngestedCoordinatesList,
    AsIngestedCoordinatesApplyList,
    AsIngestedCoordinatesFields,
    AsIngestedCoordinatesTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._as_ingested_coordinates import _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD


class AsIngestedCoordinatesFeaturesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable") -> dm.EdgeList:
        """Retrieve one or more features edges by id(s) of a as ingested coordinate.

        Args:
            external_id: External id or list of external ids source as ingested coordinate.
            space: The space where all the feature edges are located.

        Returns:
            The requested feature edges.

        Examples:

            Retrieve features edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> as_ingested_coordinate = client.as_ingested_coordinates.features.retrieve("my_features")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "AsIngestedCoordinates.features"},
        )
        if isinstance(external_id, str):
            is_as_ingested_coordinates = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
        else:
            is_as_ingested_coordinates = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_as_ingested_coordinates)
        )

    def list(
        self,
        as_ingested_coordinate_id: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List features edges of a as ingested coordinate.

        Args:
            as_ingested_coordinate_id: ID of the source as ingested coordinate.
            limit: Maximum number of feature edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the feature edges are located.

        Returns:
            The requested feature edges.

        Examples:

            List 5 features edges connected to "my_as_ingested_coordinate":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> as_ingested_coordinate = client.as_ingested_coordinates.features.list("my_as_ingested_coordinate", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "AsIngestedCoordinates.features"},
            )
        ]
        if as_ingested_coordinate_id:
            as_ingested_coordinate_ids = (
                [as_ingested_coordinate_id] if isinstance(as_ingested_coordinate_id, str) else as_ingested_coordinate_id
            )
            is_as_ingested_coordinates = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in as_ingested_coordinate_ids],
            )
            filters.append(is_as_ingested_coordinates)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class AsIngestedCoordinatesAPI(TypeAPI[AsIngestedCoordinates, AsIngestedCoordinatesApply, AsIngestedCoordinatesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AsIngestedCoordinatesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=AsIngestedCoordinates,
            class_apply_type=AsIngestedCoordinatesApply,
            class_list=AsIngestedCoordinatesList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.features = AsIngestedCoordinatesFeaturesAPI(client)

    def apply(
        self,
        as_ingested_coordinate: AsIngestedCoordinatesApply | Sequence[AsIngestedCoordinatesApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) as ingested coordinates.

        Note: This method iterates through all nodes linked to as_ingested_coordinate and create them including the edges
        between the nodes. For example, if any of `features` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            as_ingested_coordinate: As ingested coordinate or sequence of as ingested coordinates to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new as_ingested_coordinate:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import AsIngestedCoordinatesApply
                >>> client = OSDUClient()
                >>> as_ingested_coordinate = AsIngestedCoordinatesApply(external_id="my_as_ingested_coordinate", ...)
                >>> result = client.as_ingested_coordinates.apply(as_ingested_coordinate)

        """
        if isinstance(as_ingested_coordinate, AsIngestedCoordinatesApply):
            instances = as_ingested_coordinate.to_instances_apply(self._view_by_write_class)
        else:
            instances = AsIngestedCoordinatesApplyList(as_ingested_coordinate).to_instances_apply(
                self._view_by_write_class
            )
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
        """Delete one or more as ingested coordinate.

        Args:
            external_id: External id of the as ingested coordinate to delete.
            space: The space where all the as ingested coordinate are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete as_ingested_coordinate by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.as_ingested_coordinates.delete("my_as_ingested_coordinate")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> AsIngestedCoordinates:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> AsIngestedCoordinatesList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> AsIngestedCoordinates | AsIngestedCoordinatesList:
        """Retrieve one or more as ingested coordinates by id(s).

        Args:
            external_id: External id or list of external ids of the as ingested coordinates.
            space: The space where all the as ingested coordinates are located.

        Returns:
            The requested as ingested coordinates.

        Examples:

            Retrieve as_ingested_coordinate by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> as_ingested_coordinate = client.as_ingested_coordinates.retrieve("my_as_ingested_coordinate")

        """
        if isinstance(external_id, str):
            as_ingested_coordinate = self._retrieve((space, external_id))

            feature_edges = self.features.retrieve(external_id)
            as_ingested_coordinate.features = [edge.end_node.external_id for edge in feature_edges]

            return as_ingested_coordinate
        else:
            as_ingested_coordinates = self._retrieve([(space, ext_id) for ext_id in external_id])

            feature_edges = self.features.retrieve(external_id)
            self._set_features(as_ingested_coordinates, feature_edges)

            return as_ingested_coordinates

    def search(
        self,
        query: str,
        properties: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AsIngestedCoordinatesList:
        """Search as ingested coordinates

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            coordinate_reference_system_id: The coordinate reference system id to filter on.
            coordinate_reference_system_id_prefix: The prefix of the coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id: The vertical coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id_prefix: The prefix of the vertical coordinate reference system id to filter on.
            vertical_unit_id: The vertical unit id to filter on.
            vertical_unit_id_prefix: The prefix of the vertical unit id to filter on.
            persistable_reference_crs: The persistable reference cr to filter on.
            persistable_reference_crs_prefix: The prefix of the persistable reference cr to filter on.
            persistable_reference_unit_z: The persistable reference unit z to filter on.
            persistable_reference_unit_z_prefix: The prefix of the persistable reference unit z to filter on.
            persistable_reference_vertical_crs: The persistable reference vertical cr to filter on.
            persistable_reference_vertical_crs_prefix: The prefix of the persistable reference vertical cr to filter on.
            type: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of as ingested coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `features` external ids for the as ingested coordinates. Defaults to True.

        Returns:
            Search results as ingested coordinates matching the query.

        Examples:

           Search for 'my_as_ingested_coordinate' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> as_ingested_coordinates = client.as_ingested_coordinates.search('my_as_ingested_coordinate')

        """
        filter_ = _create_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
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
        property: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        group_by: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] = None,
        query: str | None = None,
        search_properties: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
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
        property: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        group_by: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        query: str | None = None,
        search_property: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across as ingested coordinates

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            coordinate_reference_system_id: The coordinate reference system id to filter on.
            coordinate_reference_system_id_prefix: The prefix of the coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id: The vertical coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id_prefix: The prefix of the vertical coordinate reference system id to filter on.
            vertical_unit_id: The vertical unit id to filter on.
            vertical_unit_id_prefix: The prefix of the vertical unit id to filter on.
            persistable_reference_crs: The persistable reference cr to filter on.
            persistable_reference_crs_prefix: The prefix of the persistable reference cr to filter on.
            persistable_reference_unit_z: The persistable reference unit z to filter on.
            persistable_reference_unit_z_prefix: The prefix of the persistable reference unit z to filter on.
            persistable_reference_vertical_crs: The persistable reference vertical cr to filter on.
            persistable_reference_vertical_crs_prefix: The prefix of the persistable reference vertical cr to filter on.
            type: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of as ingested coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `features` external ids for the as ingested coordinates. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count as ingested coordinates in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.as_ingested_coordinates.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AsIngestedCoordinatesFields,
        interval: float,
        query: str | None = None,
        search_property: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for as ingested coordinates

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            coordinate_reference_system_id: The coordinate reference system id to filter on.
            coordinate_reference_system_id_prefix: The prefix of the coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id: The vertical coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id_prefix: The prefix of the vertical coordinate reference system id to filter on.
            vertical_unit_id: The vertical unit id to filter on.
            vertical_unit_id_prefix: The prefix of the vertical unit id to filter on.
            persistable_reference_crs: The persistable reference cr to filter on.
            persistable_reference_crs_prefix: The prefix of the persistable reference cr to filter on.
            persistable_reference_unit_z: The persistable reference unit z to filter on.
            persistable_reference_unit_z_prefix: The prefix of the persistable reference unit z to filter on.
            persistable_reference_vertical_crs: The persistable reference vertical cr to filter on.
            persistable_reference_vertical_crs_prefix: The prefix of the persistable reference vertical cr to filter on.
            type: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of as ingested coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `features` external ids for the as ingested coordinates. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> AsIngestedCoordinatesList:
        """List/filter as ingested coordinates

        Args:
            coordinate_reference_system_id: The coordinate reference system id to filter on.
            coordinate_reference_system_id_prefix: The prefix of the coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id: The vertical coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id_prefix: The prefix of the vertical coordinate reference system id to filter on.
            vertical_unit_id: The vertical unit id to filter on.
            vertical_unit_id_prefix: The prefix of the vertical unit id to filter on.
            persistable_reference_crs: The persistable reference cr to filter on.
            persistable_reference_crs_prefix: The prefix of the persistable reference cr to filter on.
            persistable_reference_unit_z: The persistable reference unit z to filter on.
            persistable_reference_unit_z_prefix: The prefix of the persistable reference unit z to filter on.
            persistable_reference_vertical_crs: The persistable reference vertical cr to filter on.
            persistable_reference_vertical_crs_prefix: The prefix of the persistable reference vertical cr to filter on.
            type: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of as ingested coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `features` external ids for the as ingested coordinates. Defaults to True.

        Returns:
            List of requested as ingested coordinates

        Examples:

            List as ingested coordinates and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> as_ingested_coordinates = client.as_ingested_coordinates.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )

        as_ingested_coordinates = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := as_ingested_coordinates.as_external_ids()) > IN_FILTER_LIMIT:
                feature_edges = self.features.list(limit=-1)
            else:
                feature_edges = self.features.list(external_ids, limit=-1)
            self._set_features(as_ingested_coordinates, feature_edges)

        return as_ingested_coordinates

    @staticmethod
    def _set_features(as_ingested_coordinates: Sequence[AsIngestedCoordinates], feature_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in feature_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for as_ingested_coordinate in as_ingested_coordinates:
            node_id = as_ingested_coordinate.id_tuple()
            if node_id in edges_by_start_node:
                as_ingested_coordinate.features = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    coordinate_reference_system_id: str | list[str] | None = None,
    coordinate_reference_system_id_prefix: str | None = None,
    vertical_coordinate_reference_system_id: str | list[str] | None = None,
    vertical_coordinate_reference_system_id_prefix: str | None = None,
    vertical_unit_id: str | list[str] | None = None,
    vertical_unit_id_prefix: str | None = None,
    persistable_reference_crs: str | list[str] | None = None,
    persistable_reference_crs_prefix: str | None = None,
    persistable_reference_unit_z: str | list[str] | None = None,
    persistable_reference_unit_z_prefix: str | None = None,
    persistable_reference_vertical_crs: str | list[str] | None = None,
    persistable_reference_vertical_crs_prefix: str | None = None,
    type: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if coordinate_reference_system_id and isinstance(coordinate_reference_system_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("CoordinateReferenceSystemID"), value=coordinate_reference_system_id
            )
        )
    if coordinate_reference_system_id and isinstance(coordinate_reference_system_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("CoordinateReferenceSystemID"), values=coordinate_reference_system_id)
        )
    if coordinate_reference_system_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("CoordinateReferenceSystemID"), value=coordinate_reference_system_id_prefix
            )
        )
    if vertical_coordinate_reference_system_id and isinstance(vertical_coordinate_reference_system_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalCoordinateReferenceSystemID"),
                value=vertical_coordinate_reference_system_id,
            )
        )
    if vertical_coordinate_reference_system_id and isinstance(vertical_coordinate_reference_system_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("VerticalCoordinateReferenceSystemID"),
                values=vertical_coordinate_reference_system_id,
            )
        )
    if vertical_coordinate_reference_system_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalCoordinateReferenceSystemID"),
                value=vertical_coordinate_reference_system_id_prefix,
            )
        )
    if vertical_unit_id and isinstance(vertical_unit_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("VerticalUnitID"), value=vertical_unit_id))
    if vertical_unit_id and isinstance(vertical_unit_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("VerticalUnitID"), values=vertical_unit_id))
    if vertical_unit_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("VerticalUnitID"), value=vertical_unit_id_prefix))
    if persistable_reference_crs and isinstance(persistable_reference_crs, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("persistableReferenceCrs"), value=persistable_reference_crs)
        )
    if persistable_reference_crs and isinstance(persistable_reference_crs, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("persistableReferenceCrs"), values=persistable_reference_crs)
        )
    if persistable_reference_crs_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("persistableReferenceCrs"), value=persistable_reference_crs_prefix
            )
        )
    if persistable_reference_unit_z and isinstance(persistable_reference_unit_z, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("persistableReferenceUnitZ"), value=persistable_reference_unit_z)
        )
    if persistable_reference_unit_z and isinstance(persistable_reference_unit_z, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("persistableReferenceUnitZ"), values=persistable_reference_unit_z)
        )
    if persistable_reference_unit_z_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("persistableReferenceUnitZ"), value=persistable_reference_unit_z_prefix
            )
        )
    if persistable_reference_vertical_crs and isinstance(persistable_reference_vertical_crs, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("persistableReferenceVerticalCrs"), value=persistable_reference_vertical_crs
            )
        )
    if persistable_reference_vertical_crs and isinstance(persistable_reference_vertical_crs, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("persistableReferenceVerticalCrs"), values=persistable_reference_vertical_crs
            )
        )
    if persistable_reference_vertical_crs_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("persistableReferenceVerticalCrs"),
                value=persistable_reference_vertical_crs_prefix,
            )
        )
    if type and isinstance(type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type))
    if type and isinstance(type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
