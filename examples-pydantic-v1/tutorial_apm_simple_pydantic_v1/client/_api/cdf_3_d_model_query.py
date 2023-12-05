from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from tutorial_apm_simple_pydantic_v1.client.data_classes import (
    DomainModelApply,
    CdfModel,
    CdfModelApply,
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

from tutorial_apm_simple_pydantic_v1.client.data_classes._cdf_3_d_connection_properties import (
    _create_cdf_3_d_connection_property_filter,
)

if TYPE_CHECKING:
    from .cdf_3_d_entity_query import CdfEntityQueryAPI


class CdfModelQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("cdf_3_d_model"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[CdfModelApply], ["*"])]),
                result_cls=CdfModel,
                max_retrieve_limit=limit,
            )
        )

    def entities(
        self,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> CdfEntityQueryAPI[T_DomainModelList]:
        """Query along the entity edges of the cdf 3 d model.

        Args:
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            min_revision_node_id: The minimum value of the revision node id to filter on.
            max_revision_node_id: The maximum value of the revision node id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of entity edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            CdfEntityQueryAPI: The query API for the cdf 3 d entity.
        """
        from .cdf_3_d_entity_query import CdfEntityQueryAPI

        from_ = self._builder[-1].name

        edge_view = self._view_by_write_class[CdfConnectionPropertiesApply]
        edge_filter = _create_cdf_3_d_connection_property_filter(
            dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
            edge_view,
            min_revision_id=min_revision_id,
            max_revision_id=max_revision_id,
            min_revision_node_id=min_revision_node_id,
            max_revision_node_id=max_revision_node_id,
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("entities"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(edge_view, ["*"])],
                ),
                result_cls=CdfConnectionProperties,
                max_retrieve_limit=limit,
            )
        )
        return CdfEntityQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
