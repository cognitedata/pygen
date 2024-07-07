from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from windmill_pydantic_v1.data_classes import (
    DomainModelCore,
    Nacelle,
    Gearbox,
    Generator,
    HighSpeedShaft,
    MainShaft,
    PowerInverter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter


class NacelleQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power-models", "Nacelle", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("nacelle"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
                result_cls=Nacelle,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_gearbox: bool = False,
        retrieve_generator: bool = False,
        retrieve_high_speed_shaft: bool = False,
        retrieve_main_shaft: bool = False,
        retrieve_power_inverter: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_gearbox: Whether to retrieve the gearbox for each nacelle or not.
            retrieve_generator: Whether to retrieve the generator for each nacelle or not.
            retrieve_high_speed_shaft: Whether to retrieve the high speed shaft for each nacelle or not.
            retrieve_main_shaft: Whether to retrieve the main shaft for each nacelle or not.
            retrieve_power_inverter: Whether to retrieve the power inverter for each nacelle or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_gearbox:
            self._query_append_gearbox(from_)
        if retrieve_generator:
            self._query_append_generator(from_)
        if retrieve_high_speed_shaft:
            self._query_append_high_speed_shaft(from_)
        if retrieve_main_shaft:
            self._query_append_main_shaft(from_)
        if retrieve_power_inverter:
            self._query_append_power_inverter(from_)
        return self._query()

    def _query_append_gearbox(self, from_: str) -> None:
        view_id = Gearbox._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("gearbox"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("gearbox"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Gearbox,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_generator(self, from_: str) -> None:
        view_id = Generator._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("generator"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("generator"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Generator,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_high_speed_shaft(self, from_: str) -> None:
        view_id = HighSpeedShaft._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("high_speed_shaft"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("high_speed_shaft"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=HighSpeedShaft,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_main_shaft(self, from_: str) -> None:
        view_id = MainShaft._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("main_shaft"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("main_shaft"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=MainShaft,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_power_inverter(self, from_: str) -> None:
        view_id = PowerInverter._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("power_inverter"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("power_inverter"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PowerInverter,
                is_single_direct_relation=True,
            ),
        )
