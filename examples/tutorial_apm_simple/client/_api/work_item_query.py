from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from tutorial_apm_simple.client.data_classes import (
    WorkItem,
    WorkItemApply,
    Asset,
    AssetApply,
)
from tutorial_apm_simple.client.data_classes._work_item import (
    _WORKITEM_PROPERTIES_BY_FIELD,
)
from tutorial_apm_simple.client.data_classes._asset import (
    _ASSET_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from .asset_query import AssetQueryAPI


class WorkItemQueryAPI(QueryAPI[T_DomainModelList]):
    def linked_assets(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> AssetQueryAPI[T_DomainModelList]:
        """Query along the linked asset edges of the work item.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            AssetQueryAPI: The query API for the asset.
        """
        from .asset_query import AssetQueryAPI

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
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("asset"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[AssetApply],
                            list(_ASSET_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=Asset,
                max_retrieve_limit=-1,
            ),
        )
        return AssetQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_work_item: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_work_item: Whether to retrieve the work item or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_work_item and not self._builder[-1].name.startswith("work_item"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("work_item"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[WorkItemApply],
                                list(_WORKITEM_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=WorkItem,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
