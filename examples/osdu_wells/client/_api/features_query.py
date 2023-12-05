from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from osdu_wells.client.data_classes import (
    DomainModelApply,
    Features,
    FeaturesApply,
    Geometry,
    GeometryApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter


class FeaturesQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("feature"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[FeaturesApply], ["*"])]),
                result_cls=Features,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_geometry: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_geometry: Whether to retrieve the geometry for each feature or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_geometry:
            self._query_append_geometry(from_)
        return self._query()

    def _query_append_geometry(self, from_: str) -> None:
        view_id = self._view_by_write_class[GeometryApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("geometry"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[FeaturesApply].as_property_ref("geometry"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Geometry,
            ),
        )
