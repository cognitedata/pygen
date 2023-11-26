from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from osdu_wells.client.data_classes import (
    Well,
    WellApply,
    Meta,
    MetaApply,
)
from osdu_wells.client.data_classes._well import (
    _WELL_PROPERTIES_BY_FIELD,
)
from osdu_wells.client.data_classes._meta import (
    _META_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from .meta_query import MetaQueryAPI


class WellQueryAPI(QueryAPI[T_DomainModelList]):
    def meta(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> MetaQueryAPI[T_DomainModelList]:
        """Query along the meta edges of the well.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            MetaQueryAPI: The query API for the meta.
        """
        from .meta_query import MetaQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Well.meta"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("meta"),
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
                name=self._builder.next_name("meta"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[MetaApply],
                            list(_META_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=Meta,
                max_retrieve_limit=-1,
            ),
        )
        return MetaQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_well: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_well: Whether to retrieve the well or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_well and not self._builder[-1].name.startswith("well"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("well"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[WellApply],
                                list(_WELL_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Well,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
