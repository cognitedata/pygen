from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite_core.data_classes import (
    DomainModelCore,
    CogniteEquipment,
    CogniteAsset,
    CogniteEquipmentType,
    CogniteSourceSystem,
)
from cognite_core.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    T_DomainModelList,
    EdgeQueryStep,
    NodeQueryStep,
    DataClassQueryBuilder,
)
from cognite_core._api._core import (
    QueryAPI,
    _create_edge_filter,
)


class CogniteEquipmentQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("cdf_cdm", "CogniteEquipment", "v1")

    def __init__(
        self,
        client: CogniteClient,
        builder: DataClassQueryBuilder[T_DomainModelList],
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
                result_cls=CogniteEquipment,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_asset: bool = False,
        retrieve_equipment_type: bool = False,
        retrieve_source: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_asset: Whether to retrieve the
                asset for each
                Cognite equipment or not.
            retrieve_equipment_type: Whether to retrieve the
                equipment type for each
                Cognite equipment or not.
            retrieve_source: Whether to retrieve the
                source for each
                Cognite equipment or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_asset:
            self._query_append_asset(from_)
        if retrieve_equipment_type:
            self._query_append_equipment_type(from_)
        if retrieve_source:
            self._query_append_source(from_)
        return self._query()

    def _query_append_asset(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("asset"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteAsset._view_id]),
                ),
                result_cls=CogniteAsset,
            ),
        )

    def _query_append_equipment_type(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("equipmentType"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteEquipmentType._view_id]),
                ),
                result_cls=CogniteEquipmentType,
            ),
        )

    def _query_append_source(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("source"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteSourceSystem._view_id]),
                ),
                result_cls=CogniteSourceSystem,
            ),
        )
