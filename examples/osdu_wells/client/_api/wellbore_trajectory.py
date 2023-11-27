from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    WellboreTrajectory,
    WellboreTrajectoryApply,
    WellboreTrajectoryFields,
    WellboreTrajectoryList,
    WellboreTrajectoryApplyList,
    WellboreTrajectoryTextFields,
)
from osdu_wells.client.data_classes._wellbore_trajectory import (
    _WELLBORETRAJECTORY_PROPERTIES_BY_FIELD,
    _create_wellbore_trajectory_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .wellbore_trajectory_meta import WellboreTrajectoryMetaAPI
from .wellbore_trajectory_query import WellboreTrajectoryQueryAPI


class WellboreTrajectoryAPI(NodeAPI[WellboreTrajectory, WellboreTrajectoryApply, WellboreTrajectoryList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WellboreTrajectoryApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WellboreTrajectory,
            class_apply_type=WellboreTrajectoryApply,
            class_list=WellboreTrajectoryList,
            class_apply_list=WellboreTrajectoryApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.meta_edge = WellboreTrajectoryMetaAPI(client)

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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellboreTrajectoryQueryAPI[WellboreTrajectoryList]:
        """Query starting at wellbore trajectories.

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
            limit: Maximum number of wellbore trajectories to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for wellbore trajectories.

        """
        filter_ = _create_wellbore_trajectory_filter(
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
            WellboreTrajectoryList,
            [
                QueryStep(
                    name="wellbore_trajectory",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_WELLBORETRAJECTORY_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=WellboreTrajectory,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return WellboreTrajectoryQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, wellbore_trajectory: WellboreTrajectoryApply | Sequence[WellboreTrajectoryApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) wellbore trajectories.

        Note: This method iterates through all nodes and timeseries linked to wellbore_trajectory and creates them including the edges
        between the nodes. For example, if any of `meta` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            wellbore_trajectory: Wellbore trajectory or sequence of wellbore trajectories to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new wellbore_trajectory:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import WellboreTrajectoryApply
                >>> client = OSDUClient()
                >>> wellbore_trajectory = WellboreTrajectoryApply(external_id="my_wellbore_trajectory", ...)
                >>> result = client.wellbore_trajectory.apply(wellbore_trajectory)

        """
        return self._apply(wellbore_trajectory, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more wellbore trajectory.

        Args:
            external_id: External id of the wellbore trajectory to delete.
            space: The space where all the wellbore trajectory are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete wellbore_trajectory by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.wellbore_trajectory.delete("my_wellbore_trajectory")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> WellboreTrajectory:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> WellboreTrajectoryList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> WellboreTrajectory | WellboreTrajectoryList:
        """Retrieve one or more wellbore trajectories by id(s).

        Args:
            external_id: External id or list of external ids of the wellbore trajectories.
            space: The space where all the wellbore trajectories are located.

        Returns:
            The requested wellbore trajectories.

        Examples:

            Retrieve wellbore_trajectory by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory = client.wellbore_trajectory.retrieve("my_wellbore_trajectory")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_pairs=[
                (self.meta_edge, "meta"),
            ],
        )

    def search(
        self,
        query: str,
        properties: WellboreTrajectoryTextFields | Sequence[WellboreTrajectoryTextFields] | None = None,
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
    ) -> WellboreTrajectoryList:
        """Search wellbore trajectories

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
            limit: Maximum number of wellbore trajectories to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results wellbore trajectories matching the query.

        Examples:

           Search for 'my_wellbore_trajectory' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectories = client.wellbore_trajectory.search('my_wellbore_trajectory')

        """
        filter_ = _create_wellbore_trajectory_filter(
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
        return self._search(self._view_id, query, _WELLBORETRAJECTORY_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellboreTrajectoryFields | Sequence[WellboreTrajectoryFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WellboreTrajectoryTextFields | Sequence[WellboreTrajectoryTextFields] | None = None,
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
        property: WellboreTrajectoryFields | Sequence[WellboreTrajectoryFields] | None = None,
        group_by: WellboreTrajectoryFields | Sequence[WellboreTrajectoryFields] = None,
        query: str | None = None,
        search_properties: WellboreTrajectoryTextFields | Sequence[WellboreTrajectoryTextFields] | None = None,
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
        property: WellboreTrajectoryFields | Sequence[WellboreTrajectoryFields] | None = None,
        group_by: WellboreTrajectoryFields | Sequence[WellboreTrajectoryFields] | None = None,
        query: str | None = None,
        search_property: WellboreTrajectoryTextFields | Sequence[WellboreTrajectoryTextFields] | None = None,
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
        """Aggregate data across wellbore trajectories

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
            limit: Maximum number of wellbore trajectories to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count wellbore trajectories in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.wellbore_trajectory.aggregate("count", space="my_space")

        """

        filter_ = _create_wellbore_trajectory_filter(
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
            _WELLBORETRAJECTORY_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WellboreTrajectoryFields,
        interval: float,
        query: str | None = None,
        search_property: WellboreTrajectoryTextFields | Sequence[WellboreTrajectoryTextFields] | None = None,
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
        """Produces histograms for wellbore trajectories

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
            limit: Maximum number of wellbore trajectories to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_wellbore_trajectory_filter(
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
            _WELLBORETRAJECTORY_PROPERTIES_BY_FIELD,
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
    ) -> WellboreTrajectoryList:
        """List/filter wellbore trajectories

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
            limit: Maximum number of wellbore trajectories to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `meta` external ids for the wellbore trajectories. Defaults to True.

        Returns:
            List of requested wellbore trajectories

        Examples:

            List wellbore trajectories and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectories = client.wellbore_trajectory.list(limit=5)

        """
        filter_ = _create_wellbore_trajectory_filter(
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
            edge_api_name_pairs=[
                (self.meta_edge, "meta"),
            ],
        )
