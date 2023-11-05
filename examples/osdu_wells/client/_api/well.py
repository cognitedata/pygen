from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    Well,
    WellApply,
    WellList,
    WellApplyList,
    WellFields,
    WellTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._well import _WELL_PROPERTIES_BY_FIELD


class WellMetaAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable") -> dm.EdgeList:
        """Retrieve one or more meta edges by id(s) of a well.

        Args:
            external_id: External id or list of external ids source well.
            space: The space where all the meta edges are located.

        Returns:
            The requested meta edges.

        Examples:

            Retrieve meta edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> well = client.well.meta.retrieve("my_meta")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Well.meta"},
        )
        if isinstance(external_id, str):
            is_wells = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
        else:
            is_wells = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wells))

    def list(
        self, well_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """List meta edges of a well.

        Args:
            well_id: ID of the source well.
            limit: Maximum number of meta edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the meta edges are located.

        Returns:
            The requested meta edges.

        Examples:

            List 5 meta edges connected to "my_well":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> well = client.well.meta.list("my_well", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "Well.meta"},
            )
        ]
        if well_id:
            well_ids = [well_id] if isinstance(well_id, str) else well_id
            is_wells = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in well_ids],
            )
            filters.append(is_wells)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellAPI(TypeAPI[Well, WellApply, WellList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WellApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Well,
            class_apply_type=WellApply,
            class_list=WellList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.meta = WellMetaAPI(client)

    def apply(self, well: WellApply | Sequence[WellApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) wells.

        Note: This method iterates through all nodes linked to well and create them including the edges
        between the nodes. For example, if any of `meta` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            well: Well or sequence of wells to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new well:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import WellApply
                >>> client = OSDUClient()
                >>> well = WellApply(external_id="my_well", ...)
                >>> result = client.well.apply(well)

        """
        if isinstance(well, WellApply):
            instances = well.to_instances_apply(self._view_by_write_class)
        else:
            instances = WellApplyList(well).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more well.

        Args:
            external_id: External id of the well to delete.
            space: The space where all the well are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete well by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.well.delete("my_well")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Well:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WellList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable") -> Well | WellList:
        """Retrieve one or more wells by id(s).

        Args:
            external_id: External id or list of external ids of the wells.
            space: The space where all the wells are located.

        Returns:
            The requested wells.

        Examples:

            Retrieve well by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> well = client.well.retrieve("my_well")

        """
        if isinstance(external_id, str):
            well = self._retrieve((space, external_id))

            meta_edges = self.meta.retrieve(external_id, space=space)
            well.meta = [edge.end_node.external_id for edge in meta_edges]

            return well
        else:
            wells = self._retrieve([(space, ext_id) for ext_id in external_id])

            meta_edges = self.meta.retrieve(external_id, space=space)
            self._set_meta(wells, meta_edges)

            return wells

    def search(
        self,
        query: str,
        properties: WellTextFields | Sequence[WellTextFields] | None = None,
        acl: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        ancestry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        create_time: str | list[str] | None = None,
        create_time_prefix: str | None = None,
        create_user: str | list[str] | None = None,
        create_user_prefix: str | None = None,
        data: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version: int | None = None,
        max_version: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellList:
        """Search wells

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            acl: The acl to filter on.
            ancestry: The ancestry to filter on.
            create_time: The create time to filter on.
            create_time_prefix: The prefix of the create time to filter on.
            create_user: The create user to filter on.
            create_user_prefix: The prefix of the create user to filter on.
            data: The datum to filter on.
            id: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            legal: The legal to filter on.
            modify_time: The modify time to filter on.
            modify_time_prefix: The prefix of the modify time to filter on.
            modify_user: The modify user to filter on.
            modify_user_prefix: The prefix of the modify user to filter on.
            tags: The tag to filter on.
            min_version: The minimum value of the version to filter on.
            max_version: The maximum value of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wells to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `meta` external ids for the wells. Defaults to True.

        Returns:
            Search results wells matching the query.

        Examples:

           Search for 'my_well' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wells = client.well.search('my_well')

        """
        filter_ = _create_filter(
            self._view_id,
            acl,
            ancestry,
            create_time,
            create_time_prefix,
            create_user,
            create_user_prefix,
            data,
            id,
            id_prefix,
            kind,
            kind_prefix,
            legal,
            modify_time,
            modify_time_prefix,
            modify_user,
            modify_user_prefix,
            tags,
            min_version,
            max_version,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WELL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellFields | Sequence[WellFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WellTextFields | Sequence[WellTextFields] | None = None,
        acl: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        ancestry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        create_time: str | list[str] | None = None,
        create_time_prefix: str | None = None,
        create_user: str | list[str] | None = None,
        create_user_prefix: str | None = None,
        data: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version: int | None = None,
        max_version: int | None = None,
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
        property: WellFields | Sequence[WellFields] | None = None,
        group_by: WellFields | Sequence[WellFields] = None,
        query: str | None = None,
        search_properties: WellTextFields | Sequence[WellTextFields] | None = None,
        acl: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        ancestry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        create_time: str | list[str] | None = None,
        create_time_prefix: str | None = None,
        create_user: str | list[str] | None = None,
        create_user_prefix: str | None = None,
        data: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version: int | None = None,
        max_version: int | None = None,
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
        property: WellFields | Sequence[WellFields] | None = None,
        group_by: WellFields | Sequence[WellFields] | None = None,
        query: str | None = None,
        search_property: WellTextFields | Sequence[WellTextFields] | None = None,
        acl: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        ancestry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        create_time: str | list[str] | None = None,
        create_time_prefix: str | None = None,
        create_user: str | list[str] | None = None,
        create_user_prefix: str | None = None,
        data: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version: int | None = None,
        max_version: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across wells

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            acl: The acl to filter on.
            ancestry: The ancestry to filter on.
            create_time: The create time to filter on.
            create_time_prefix: The prefix of the create time to filter on.
            create_user: The create user to filter on.
            create_user_prefix: The prefix of the create user to filter on.
            data: The datum to filter on.
            id: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            legal: The legal to filter on.
            modify_time: The modify time to filter on.
            modify_time_prefix: The prefix of the modify time to filter on.
            modify_user: The modify user to filter on.
            modify_user_prefix: The prefix of the modify user to filter on.
            tags: The tag to filter on.
            min_version: The minimum value of the version to filter on.
            max_version: The maximum value of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wells to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `meta` external ids for the wells. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count wells in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.well.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            acl,
            ancestry,
            create_time,
            create_time_prefix,
            create_user,
            create_user_prefix,
            data,
            id,
            id_prefix,
            kind,
            kind_prefix,
            legal,
            modify_time,
            modify_time_prefix,
            modify_user,
            modify_user_prefix,
            tags,
            min_version,
            max_version,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WELL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WellFields,
        interval: float,
        query: str | None = None,
        search_property: WellTextFields | Sequence[WellTextFields] | None = None,
        acl: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        ancestry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        create_time: str | list[str] | None = None,
        create_time_prefix: str | None = None,
        create_user: str | list[str] | None = None,
        create_user_prefix: str | None = None,
        data: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version: int | None = None,
        max_version: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for wells

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            acl: The acl to filter on.
            ancestry: The ancestry to filter on.
            create_time: The create time to filter on.
            create_time_prefix: The prefix of the create time to filter on.
            create_user: The create user to filter on.
            create_user_prefix: The prefix of the create user to filter on.
            data: The datum to filter on.
            id: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            legal: The legal to filter on.
            modify_time: The modify time to filter on.
            modify_time_prefix: The prefix of the modify time to filter on.
            modify_user: The modify user to filter on.
            modify_user_prefix: The prefix of the modify user to filter on.
            tags: The tag to filter on.
            min_version: The minimum value of the version to filter on.
            max_version: The maximum value of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wells to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `meta` external ids for the wells. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            acl,
            ancestry,
            create_time,
            create_time_prefix,
            create_user,
            create_user_prefix,
            data,
            id,
            id_prefix,
            kind,
            kind_prefix,
            legal,
            modify_time,
            modify_time_prefix,
            modify_user,
            modify_user_prefix,
            tags,
            min_version,
            max_version,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WELL_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        acl: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        ancestry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        create_time: str | list[str] | None = None,
        create_time_prefix: str | None = None,
        create_user: str | list[str] | None = None,
        create_user_prefix: str | None = None,
        data: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version: int | None = None,
        max_version: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WellList:
        """List/filter wells

        Args:
            acl: The acl to filter on.
            ancestry: The ancestry to filter on.
            create_time: The create time to filter on.
            create_time_prefix: The prefix of the create time to filter on.
            create_user: The create user to filter on.
            create_user_prefix: The prefix of the create user to filter on.
            data: The datum to filter on.
            id: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            legal: The legal to filter on.
            modify_time: The modify time to filter on.
            modify_time_prefix: The prefix of the modify time to filter on.
            modify_user: The modify user to filter on.
            modify_user_prefix: The prefix of the modify user to filter on.
            tags: The tag to filter on.
            min_version: The minimum value of the version to filter on.
            max_version: The maximum value of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wells to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `meta` external ids for the wells. Defaults to True.

        Returns:
            List of requested wells

        Examples:

            List wells and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wells = client.well.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            acl,
            ancestry,
            create_time,
            create_time_prefix,
            create_user,
            create_user_prefix,
            data,
            id,
            id_prefix,
            kind,
            kind_prefix,
            legal,
            modify_time,
            modify_time_prefix,
            modify_user,
            modify_user_prefix,
            tags,
            min_version,
            max_version,
            external_id_prefix,
            space,
            filter,
        )

        wells = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := wells.as_external_ids()) > IN_FILTER_LIMIT:
                meta_edges = self.meta.list(limit=-1, space=space)
            else:
                meta_edges = self.meta.list(external_ids, limit=-1, space=space)
            self._set_meta(wells, meta_edges)

        return wells

    @staticmethod
    def _set_meta(wells: Sequence[Well], meta_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in meta_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for well in wells:
            node_id = well.id_tuple()
            if node_id in edges_by_start_node:
                well.meta = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    acl: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    ancestry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    create_time: str | list[str] | None = None,
    create_time_prefix: str | None = None,
    create_user: str | list[str] | None = None,
    create_user_prefix: str | None = None,
    data: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    id: str | list[str] | None = None,
    id_prefix: str | None = None,
    kind: str | list[str] | None = None,
    kind_prefix: str | None = None,
    legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    modify_time: str | list[str] | None = None,
    modify_time_prefix: str | None = None,
    modify_user: str | list[str] | None = None,
    modify_user_prefix: str | None = None,
    tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_version: int | None = None,
    max_version: int | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if acl and isinstance(acl, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("acl"), value={"space": "IntegrationTestsImmutable", "externalId": acl}
            )
        )
    if acl and isinstance(acl, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("acl"), value={"space": acl[0], "externalId": acl[1]}))
    if acl and isinstance(acl, list) and isinstance(acl[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("acl"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in acl],
            )
        )
    if acl and isinstance(acl, list) and isinstance(acl[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("acl"), values=[{"space": item[0], "externalId": item[1]} for item in acl]
            )
        )
    if ancestry and isinstance(ancestry, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("ancestry"),
                value={"space": "IntegrationTestsImmutable", "externalId": ancestry},
            )
        )
    if ancestry and isinstance(ancestry, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("ancestry"), value={"space": ancestry[0], "externalId": ancestry[1]}
            )
        )
    if ancestry and isinstance(ancestry, list) and isinstance(ancestry[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("ancestry"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in ancestry],
            )
        )
    if ancestry and isinstance(ancestry, list) and isinstance(ancestry[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("ancestry"),
                values=[{"space": item[0], "externalId": item[1]} for item in ancestry],
            )
        )
    if create_time and isinstance(create_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("createTime"), value=create_time))
    if create_time and isinstance(create_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("createTime"), values=create_time))
    if create_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("createTime"), value=create_time_prefix))
    if create_user and isinstance(create_user, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("createUser"), value=create_user))
    if create_user and isinstance(create_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("createUser"), values=create_user))
    if create_user_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("createUser"), value=create_user_prefix))
    if data and isinstance(data, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("data"), value={"space": "IntegrationTestsImmutable", "externalId": data}
            )
        )
    if data and isinstance(data, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("data"), value={"space": data[0], "externalId": data[1]})
        )
    if data and isinstance(data, list) and isinstance(data[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("data"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in data],
            )
        )
    if data and isinstance(data, list) and isinstance(data[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("data"), values=[{"space": item[0], "externalId": item[1]} for item in data]
            )
        )
    if id and isinstance(id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("id"), value=id))
    if id and isinstance(id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("id"), values=id))
    if id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("id"), value=id_prefix))
    if kind and isinstance(kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("kind"), value=kind))
    if kind and isinstance(kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("kind"), values=kind))
    if kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("kind"), value=kind_prefix))
    if legal and isinstance(legal, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("legal"), value={"space": "IntegrationTestsImmutable", "externalId": legal}
            )
        )
    if legal and isinstance(legal, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("legal"), value={"space": legal[0], "externalId": legal[1]})
        )
    if legal and isinstance(legal, list) and isinstance(legal[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("legal"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in legal],
            )
        )
    if legal and isinstance(legal, list) and isinstance(legal[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("legal"), values=[{"space": item[0], "externalId": item[1]} for item in legal]
            )
        )
    if modify_time and isinstance(modify_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("modifyTime"), value=modify_time))
    if modify_time and isinstance(modify_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("modifyTime"), values=modify_time))
    if modify_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("modifyTime"), value=modify_time_prefix))
    if modify_user and isinstance(modify_user, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("modifyUser"), value=modify_user))
    if modify_user and isinstance(modify_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("modifyUser"), values=modify_user))
    if modify_user_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("modifyUser"), value=modify_user_prefix))
    if tags and isinstance(tags, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("tags"), value={"space": "IntegrationTestsImmutable", "externalId": tags}
            )
        )
    if tags and isinstance(tags, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("tags"), value={"space": tags[0], "externalId": tags[1]})
        )
    if tags and isinstance(tags, list) and isinstance(tags[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("tags"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in tags],
            )
        )
    if tags and isinstance(tags, list) and isinstance(tags[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("tags"), values=[{"space": item[0], "externalId": item[1]} for item in tags]
            )
        )
    if min_version or max_version:
        filters.append(dm.filters.Range(view_id.as_property_ref("version"), gte=min_version, lte=max_version))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
