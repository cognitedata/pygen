from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite_core.data_classes import (
    DomainModelCore,
    Cognite360Image,
    CogniteFile,
    CogniteFile,
    Cognite360ImageCollection,
    CogniteFile,
    CogniteFile,
    CogniteFile,
    Cognite360ImageStation,
    CogniteFile,
)
from cognite_core.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from cognite_core._api._core import (
    QueryAPI,
    _create_edge_filter,
)


class Cognite360ImageQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("cdf_cdm", "Cognite360Image", "v1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder,
        result_cls: type[T_DomainModel],
        result_list_cls: type[T_DomainModelList],
        connection_property: ViewPropertyId | None = None,
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, result_cls, result_list_cls)
        from_ = self._builder.get_from()
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                max_retrieve_limit=limit,
                view_id=self._view_id,
                connection_property=connection_property,
            )
        )

    def query(
        self,
        retrieve_back: bool = False,
        retrieve_bottom: bool = False,
        retrieve_collection_360: bool = False,
        retrieve_front: bool = False,
        retrieve_left: bool = False,
        retrieve_right: bool = False,
        retrieve_station_360: bool = False,
        retrieve_top: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_back: Whether to retrieve the
                back for each
                Cognite 360 image or not.
            retrieve_bottom: Whether to retrieve the
                bottom for each
                Cognite 360 image or not.
            retrieve_collection_360: Whether to retrieve the
                collection 360 for each
                Cognite 360 image or not.
            retrieve_front: Whether to retrieve the
                front for each
                Cognite 360 image or not.
            retrieve_left: Whether to retrieve the
                left for each
                Cognite 360 image or not.
            retrieve_right: Whether to retrieve the
                right for each
                Cognite 360 image or not.
            retrieve_station_360: Whether to retrieve the
                station 360 for each
                Cognite 360 image or not.
            retrieve_top: Whether to retrieve the
                top for each
                Cognite 360 image or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_back:
            self._query_append_back(from_)
        if retrieve_bottom:
            self._query_append_bottom(from_)
        if retrieve_collection_360:
            self._query_append_collection_360(from_)
        if retrieve_front:
            self._query_append_front(from_)
        if retrieve_left:
            self._query_append_left(from_)
        if retrieve_right:
            self._query_append_right(from_)
        if retrieve_station_360:
            self._query_append_station_360(from_)
        if retrieve_top:
            self._query_append_top(from_)
        return self._query()

    def _query_append_back(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("back"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                view_id=CogniteFile._view_id,
                connection_property=ViewPropertyId(self._view_id, "back"),
            ),
        )

    def _query_append_bottom(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("bottom"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                view_id=CogniteFile._view_id,
                connection_property=ViewPropertyId(self._view_id, "bottom"),
            ),
        )

    def _query_append_collection_360(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("collection360"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Cognite360ImageCollection._view_id]),
                ),
                view_id=Cognite360ImageCollection._view_id,
                connection_property=ViewPropertyId(self._view_id, "collection360"),
            ),
        )

    def _query_append_front(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("front"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                view_id=CogniteFile._view_id,
                connection_property=ViewPropertyId(self._view_id, "front"),
            ),
        )

    def _query_append_left(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("left"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                view_id=CogniteFile._view_id,
                connection_property=ViewPropertyId(self._view_id, "left"),
            ),
        )

    def _query_append_right(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("right"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                view_id=CogniteFile._view_id,
                connection_property=ViewPropertyId(self._view_id, "right"),
            ),
        )

    def _query_append_station_360(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("station360"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Cognite360ImageStation._view_id]),
                ),
                view_id=Cognite360ImageStation._view_id,
                connection_property=ViewPropertyId(self._view_id, "station360"),
            ),
        )

    def _query_append_top(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("top"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                view_id=CogniteFile._view_id,
                connection_property=ViewPropertyId(self._view_id, "top"),
            ),
        )
