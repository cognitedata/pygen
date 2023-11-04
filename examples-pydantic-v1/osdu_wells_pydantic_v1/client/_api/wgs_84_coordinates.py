from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    WgsCoordinates,
    WgsCoordinatesApply,
    WgsCoordinatesList,
    WgsCoordinatesApplyList,
    WgsCoordinatesFields,
    WgsCoordinatesTextFields,
    DomainModelApply,
)
from osdu_wells_pydantic_v1.client.data_classes._wgs_84_coordinates import _WGSCOORDINATES_PROPERTIES_BY_FIELD


class WgsCoordinatesFeaturesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        """Retrieve one or more features edges by id(s) of a wgs 84 coordinate.

        Args:
            external_id: External id or list of external ids source wgs 84 coordinate.
            space: The space where all the feature edges are located.

        Returns:
            The requested feature edges.

        Examples:

            Retrieve features edge by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wgs_84_coordinate = client.wgs_84_coordinates.features.retrieve("my_features")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Wgs84Coordinates.features"},
        )
        if isinstance(external_id, str):
            is_wgs_84_coordinate = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_wgs_84_coordinate)
            )

        else:
            is_wgs_84_coordinates = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_wgs_84_coordinates)
            )

    def list(
        self,
        wgs_84_coordinate_id: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space="IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List features edges of a wgs 84 coordinate.

        Args:
            wgs_84_coordinate_id: Id of the source wgs 84 coordinate.
            limit: Maximum number of feature edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the feature edges are located.

        Returns:
            The requested feature edges.

        Examples:

            List 5 features edges connected to "my_wgs_84_coordinate":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wgs_84_coordinate = client.wgs_84_coordinates.features.list("my_wgs_84_coordinate", limit=5)

        """
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Wgs84Coordinates.features"},
        )
        filters.append(is_edge_type)
        if wgs_84_coordinate_id:
            wgs_84_coordinate_ids = (
                [wgs_84_coordinate_id] if isinstance(wgs_84_coordinate_id, str) else wgs_84_coordinate_id
            )
            is_wgs_84_coordinates = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in wgs_84_coordinate_ids],
            )
            filters.append(is_wgs_84_coordinates)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WgsCoordinatesAPI(TypeAPI[WgsCoordinates, WgsCoordinatesApply, WgsCoordinatesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WgsCoordinatesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WgsCoordinates,
            class_apply_type=WgsCoordinatesApply,
            class_list=WgsCoordinatesList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.features = WgsCoordinatesFeaturesAPI(client)

    def apply(
        self, wgs_84_coordinate: WgsCoordinatesApply | Sequence[WgsCoordinatesApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) wgs 84 coordinates.

        Note: This method iterates through all nodes linked to wgs_84_coordinate and create them including the edges
        between the nodes. For example, if any of `features` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            wgs_84_coordinate: Wgs 84 coordinate or sequence of wgs 84 coordinates to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new wgs_84_coordinate:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import WgsCoordinatesApply
                >>> client = OSDUClient()
                >>> wgs_84_coordinate = WgsCoordinatesApply(external_id="my_wgs_84_coordinate", ...)
                >>> result = client.wgs_84_coordinates.apply(wgs_84_coordinate)

        """
        if isinstance(wgs_84_coordinate, WgsCoordinatesApply):
            instances = wgs_84_coordinate.to_instances_apply(self._view_by_write_class)
        else:
            instances = WgsCoordinatesApplyList(wgs_84_coordinate).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more wgs 84 coordinate.

        Args:
            external_id: External id of the wgs 84 coordinate to delete.
            space: The space where all the wgs 84 coordinate are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete wgs_84_coordinate by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.wgs_84_coordinates.delete("my_wgs_84_coordinate")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WgsCoordinates:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WgsCoordinatesList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> WgsCoordinates | WgsCoordinatesList:
        """Retrieve one or more wgs 84 coordinates by id(s).

        Args:
            external_id: External id or list of external ids of the wgs 84 coordinates.
            space: The space where all the wgs 84 coordinates are located.

        Returns:
            The requested wgs 84 coordinates.

        Examples:

            Retrieve wgs_84_coordinate by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wgs_84_coordinate = client.wgs_84_coordinates.retrieve("my_wgs_84_coordinate")

        """
        if isinstance(external_id, str):
            wgs_84_coordinate = self._retrieve((space, external_id))

            feature_edges = self.features.retrieve(external_id)
            wgs_84_coordinate.features = [edge.end_node.external_id for edge in feature_edges]

            return wgs_84_coordinate
        else:
            wgs_84_coordinates = self._retrieve([(space, ext_id) for ext_id in external_id])

            feature_edges = self.features.retrieve(external_id)
            self._set_features(wgs_84_coordinates, feature_edges)

            return wgs_84_coordinates

    def search(
        self,
        query: str,
        properties: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WgsCoordinatesList:
        """Search wgs 84 coordinates

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            type: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wgs 84 coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `features` external ids for the wgs 84 coordinates. Defaults to True.

        Returns:
            Search results wgs 84 coordinates matching the query.

        Examples:

           Search for 'my_wgs_84_coordinate' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wgs_84_coordinates = client.wgs_84_coordinates.search('my_wgs_84_coordinate')

        """
        filter_ = _create_filter(
            self._view_id,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WGSCOORDINATES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
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
        property: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        group_by: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] = None,
        query: str | None = None,
        search_properties: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
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
        property: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        group_by: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        query: str | None = None,
        search_property: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across wgs 84 coordinates

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            type: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wgs 84 coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `features` external ids for the wgs 84 coordinates. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count wgs 84 coordinates in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.wgs_84_coordinates.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WGSCOORDINATES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WgsCoordinatesFields,
        interval: float,
        query: str | None = None,
        search_property: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for wgs 84 coordinates

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            type: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wgs 84 coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `features` external ids for the wgs 84 coordinates. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
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
            _WGSCOORDINATES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WgsCoordinatesList:
        """List/filter wgs 84 coordinates

        Args:
            type: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wgs 84 coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `features` external ids for the wgs 84 coordinates. Defaults to True.

        Returns:
            List of requested wgs 84 coordinates

        Examples:

            List wgs 84 coordinates and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wgs_84_coordinates = client.wgs_84_coordinates.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )

        wgs_84_coordinates = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := wgs_84_coordinates.as_external_ids()) > IN_FILTER_LIMIT:
                feature_edges = self.features.list(limit=-1)
            else:
                feature_edges = self.features.list(external_ids, limit=-1)
            self._set_features(wgs_84_coordinates, feature_edges)

        return wgs_84_coordinates

    @staticmethod
    def _set_features(wgs_84_coordinates: Sequence[WgsCoordinates], feature_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in feature_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wgs_84_coordinate in wgs_84_coordinates:
            node_id = wgs_84_coordinate.id_tuple()
            if node_id in edges_by_start_node:
                wgs_84_coordinate.features = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    type: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
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
