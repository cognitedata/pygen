from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Well,
    WellApply,
    WellFields,
    WellList,
    WellApplyList,
    WellTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._well import (
    _WELL_PROPERTIES_BY_FIELD,
    _create_well_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .well_meta import WellMetaAPI
from .well_query import WellQueryAPI


class WellAPI(NodeAPI[Well, WellApply, WellList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WellApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Well,
            class_apply_type=WellApply,
            class_list=WellList,
            class_apply_list=WellApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.meta_edge = WellMetaAPI(client)

    def __call__(
        self,
        acl: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        ancestry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        create_time: str | list[str] | None = None,
        create_time_prefix: str | None = None,
        create_user: str | list[str] | None = None,
        create_user_prefix: str | None = None,
        data: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version_: int | None = None,
        max_version_: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WellQueryAPI[WellList]:
        """Query starting at wells.

        Args:
            acl: The acl to filter on.
            ancestry: The ancestry to filter on.
            create_time: The create time to filter on.
            create_time_prefix: The prefix of the create time to filter on.
            create_user: The create user to filter on.
            create_user_prefix: The prefix of the create user to filter on.
            data: The datum to filter on.
            id_: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            legal: The legal to filter on.
            modify_time: The modify time to filter on.
            modify_time_prefix: The prefix of the modify time to filter on.
            modify_user: The modify user to filter on.
            modify_user_prefix: The prefix of the modify user to filter on.
            tags: The tag to filter on.
            min_version_: The minimum value of the version to filter on.
            max_version_: The maximum value of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wells to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for wells.

        """
        filter_ = _create_well_filter(
            self._view_id,
            acl,
            ancestry,
            create_time,
            create_time_prefix,
            create_user,
            create_user_prefix,
            data,
            id_,
            id_prefix,
            kind,
            kind_prefix,
            legal,
            modify_time,
            modify_time_prefix,
            modify_user,
            modify_user_prefix,
            tags,
            min_version_,
            max_version_,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            WellList,
            [
                QueryStep(
                    name="well",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_WELL_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Well,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return WellQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, well: WellApply | Sequence[WellApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) wells.

        Note: This method iterates through all nodes and timeseries linked to well and creates them including the edges
        between the nodes. For example, if any of `meta` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            well: Well or sequence of wells to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new well:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import WellApply
                >>> client = OSDUClient()
                >>> well = WellApply(external_id="my_well", ...)
                >>> result = client.well.apply(well)

        """
        return self._apply(well, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more well.

        Args:
            external_id: External id of the well to delete.
            space: The space where all the well are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete well by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.well.delete("my_well")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Well:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> WellList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> Well | WellList:
        """Retrieve one or more wells by id(s).

        Args:
            external_id: External id or list of external ids of the wells.
            space: The space where all the wells are located.

        Returns:
            The requested wells.

        Examples:

            Retrieve well by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> well = client.well.retrieve("my_well")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_triple=[
                (self.meta_edge, "meta", dm.DirectRelationReference("IntegrationTestsImmutable", "Well.meta")),
            ],
        )

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
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version_: int | None = None,
        max_version_: int | None = None,
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
            id_: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            legal: The legal to filter on.
            modify_time: The modify time to filter on.
            modify_time_prefix: The prefix of the modify time to filter on.
            modify_user: The modify user to filter on.
            modify_user_prefix: The prefix of the modify user to filter on.
            tags: The tag to filter on.
            min_version_: The minimum value of the version to filter on.
            max_version_: The maximum value of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wells to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results wells matching the query.

        Examples:

           Search for 'my_well' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wells = client.well.search('my_well')

        """
        filter_ = _create_well_filter(
            self._view_id,
            acl,
            ancestry,
            create_time,
            create_time_prefix,
            create_user,
            create_user_prefix,
            data,
            id_,
            id_prefix,
            kind,
            kind_prefix,
            legal,
            modify_time,
            modify_time_prefix,
            modify_user,
            modify_user_prefix,
            tags,
            min_version_,
            max_version_,
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
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version_: int | None = None,
        max_version_: int | None = None,
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
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version_: int | None = None,
        max_version_: int | None = None,
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
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version_: int | None = None,
        max_version_: int | None = None,
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
            id_: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            legal: The legal to filter on.
            modify_time: The modify time to filter on.
            modify_time_prefix: The prefix of the modify time to filter on.
            modify_user: The modify user to filter on.
            modify_user_prefix: The prefix of the modify user to filter on.
            tags: The tag to filter on.
            min_version_: The minimum value of the version to filter on.
            max_version_: The maximum value of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wells to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count wells in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.well.aggregate("count", space="my_space")

        """

        filter_ = _create_well_filter(
            self._view_id,
            acl,
            ancestry,
            create_time,
            create_time_prefix,
            create_user,
            create_user_prefix,
            data,
            id_,
            id_prefix,
            kind,
            kind_prefix,
            legal,
            modify_time,
            modify_time_prefix,
            modify_user,
            modify_user_prefix,
            tags,
            min_version_,
            max_version_,
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
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version_: int | None = None,
        max_version_: int | None = None,
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
            id_: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            legal: The legal to filter on.
            modify_time: The modify time to filter on.
            modify_time_prefix: The prefix of the modify time to filter on.
            modify_user: The modify user to filter on.
            modify_user_prefix: The prefix of the modify user to filter on.
            tags: The tag to filter on.
            min_version_: The minimum value of the version to filter on.
            max_version_: The maximum value of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wells to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_well_filter(
            self._view_id,
            acl,
            ancestry,
            create_time,
            create_time_prefix,
            create_user,
            create_user_prefix,
            data,
            id_,
            id_prefix,
            kind,
            kind_prefix,
            legal,
            modify_time,
            modify_time_prefix,
            modify_user,
            modify_user_prefix,
            tags,
            min_version_,
            max_version_,
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
        id_: str | list[str] | None = None,
        id_prefix: str | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        modify_time: str | list[str] | None = None,
        modify_time_prefix: str | None = None,
        modify_user: str | list[str] | None = None,
        modify_user_prefix: str | None = None,
        tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_version_: int | None = None,
        max_version_: int | None = None,
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
            id_: The id to filter on.
            id_prefix: The prefix of the id to filter on.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            legal: The legal to filter on.
            modify_time: The modify time to filter on.
            modify_time_prefix: The prefix of the modify time to filter on.
            modify_user: The modify user to filter on.
            modify_user_prefix: The prefix of the modify user to filter on.
            tags: The tag to filter on.
            min_version_: The minimum value of the version to filter on.
            max_version_: The maximum value of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wells to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `meta` external ids for the wells. Defaults to True.

        Returns:
            List of requested wells

        Examples:

            List wells and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wells = client.well.list(limit=5)

        """
        filter_ = _create_well_filter(
            self._view_id,
            acl,
            ancestry,
            create_time,
            create_time_prefix,
            create_user,
            create_user_prefix,
            data,
            id_,
            id_prefix,
            kind,
            kind_prefix,
            legal,
            modify_time,
            modify_time_prefix,
            modify_user,
            modify_user_prefix,
            tags,
            min_version_,
            max_version_,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_triple=[
                (self.meta_edge, "meta", dm.DirectRelationReference("IntegrationTestsImmutable", "Well.meta")),
            ],
        )
