from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from equipment_unit.data_classes import (
    DomainModelCore,
    EquipmentModule,
)
from ._core import (
    DEFAULT_QUERY_LIMIT,
    EdgeQueryStep,
    NodeQueryStep,
    QueryBuilder,
    QueryAPI,
    T_DomainModelList,
    _create_edge_filter,
)



class EquipmentModuleQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("IntegrationTestsImmutable", "EquipmentModule", "b1cd4bf14a7a33")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)
        from_ = self._builder.get_from()
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                result_cls=EquipmentModule,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()

