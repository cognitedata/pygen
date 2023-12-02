from __future__ import annotations

from cognite.client import data_modeling as dm, CogniteClient
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList, QueryBuilder
from equipment_unit.client.data_classes import (
    EquipmentModule,
    EquipmentModuleApply,
    DomainModelApply,
)


class EquipmentModuleQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("equipment_module"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[EquipmentModuleApply],
                            ["*"],
                        )
                    ]
                ),
                result_cls=EquipmentModule,
                max_retrieve_limit=limit,
            ),
        )

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
