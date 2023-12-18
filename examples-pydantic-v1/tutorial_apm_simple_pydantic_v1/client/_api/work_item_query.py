from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from tutorial_apm_simple_pydantic_v1.client.data_classes import (
    DomainModelApply,
    WorkItem,
    WorkItemApply,
    WorkOrder,
    WorkOrderApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .asset_query import AssetQueryAPI


class WorkItemQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_write_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("work_item"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[WorkItemApply], ["*"])]),
                result_cls=WorkItem,
                max_retrieve_limit=limit,
            )
        )

    def linked_assets(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_work_order: bool = False,
    ) -> AssetQueryAPI[T_DomainModelList]:
        """Query along the linked asset edges of the work item.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of linked asset edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_work_order: Whether to retrieve the work order for each work item or not.

        Returns:
            AssetQueryAPI: The query API for the asset.
        """
        from .asset_query import AssetQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("tutorial_apm_simple", "WorkItem.linkedAssets"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("linked_assets"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_work_order:
            self._query_append_work_order(from_)
        return AssetQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
        retrieve_work_order: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_work_order: Whether to retrieve the work order for each work item or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_work_order:
            self._query_append_work_order(from_)
        return self._query()

    def _query_append_work_order(self, from_: str) -> None:
        view_id = self._view_by_write_class[WorkOrderApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("work_order"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[WorkItemApply].as_property_ref("work_order"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=WorkOrder,
            ),
        )
