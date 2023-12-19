from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from markets.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from markets.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    DateTransformationPair,
    DateTransformationPairApply,
    DateTransformationPairList,
    DateTransformationPairApplyList,
)
from markets.client.data_classes._date_transformation_pair import (
    _create_date_transformation_pair_filter,
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
from .date_transformation_pair_end import DateTransformationPairEndAPI
from .date_transformation_pair_start import DateTransformationPairStartAPI
from .date_transformation_pair_query import DateTransformationPairQueryAPI


class DateTransformationPairAPI(
    NodeAPI[DateTransformationPair, DateTransformationPairApply, DateTransformationPairList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[DateTransformationPairApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DateTransformationPair,
            class_apply_type=DateTransformationPairApply,
            class_list=DateTransformationPairList,
            class_apply_list=DateTransformationPairApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.end_edge = DateTransformationPairEndAPI(client)
        self.start_edge = DateTransformationPairStartAPI(client)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> DateTransformationPairQueryAPI[DateTransformationPairList]:
        """Query starting at date transformation pairs.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of date transformation pairs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for date transformation pairs.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_date_transformation_pair_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(DateTransformationPairList)
        return DateTransformationPairQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self,
        date_transformation_pair: DateTransformationPairApply | Sequence[DateTransformationPairApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) date transformation pairs.

        Note: This method iterates through all nodes and timeseries linked to date_transformation_pair and creates them including the edges
        between the nodes. For example, if any of `end` or `start` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            date_transformation_pair: Date transformation pair or sequence of date transformation pairs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new date_transformation_pair:

                >>> from markets.client import MarketClient
                >>> from markets.client.data_classes import DateTransformationPairApply
                >>> client = MarketClient()
                >>> date_transformation_pair = DateTransformationPairApply(external_id="my_date_transformation_pair", ...)
                >>> result = client.date_transformation_pair.apply(date_transformation_pair)

        """
        return self._apply(date_transformation_pair, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more date transformation pair.

        Args:
            external_id: External id of the date transformation pair to delete.
            space: The space where all the date transformation pair are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete date_transformation_pair by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> client.date_transformation_pair.delete("my_date_transformation_pair")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> DateTransformationPair | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> DateTransformationPairList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> DateTransformationPair | DateTransformationPairList | None:
        """Retrieve one or more date transformation pairs by id(s).

        Args:
            external_id: External id or list of external ids of the date transformation pairs.
            space: The space where all the date transformation pairs are located.

        Returns:
            The requested date transformation pairs.

        Examples:

            Retrieve date_transformation_pair by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> date_transformation_pair = client.date_transformation_pair.retrieve("my_date_transformation_pair")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_quad=[
                (
                    self.end_edge,
                    "end",
                    dm.DirectRelationReference("market", "DateTransformationPair.end"),
                    "outwards",
                ),
                (
                    self.start_edge,
                    "start",
                    dm.DirectRelationReference("market", "DateTransformationPair.start"),
                    "outwards",
                ),
            ],
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> DateTransformationPairList:
        """List/filter date transformation pairs

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of date transformation pairs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `end` or `start` external ids for the date transformation pairs. Defaults to True.

        Returns:
            List of requested date transformation pairs

        Examples:

            List date transformation pairs and limit to 5:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> date_transformation_pairs = client.date_transformation_pair.list(limit=5)

        """
        filter_ = _create_date_transformation_pair_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_quad=[
                (
                    self.end_edge,
                    "end",
                    dm.DirectRelationReference("market", "DateTransformationPair.end"),
                    "outwards",
                ),
                (
                    self.start_edge,
                    "start",
                    dm.DirectRelationReference("market", "DateTransformationPair.start"),
                    "outwards",
                ),
            ],
        )
