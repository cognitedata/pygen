from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from windmill_pydantic_v1.data_classes import (
    DomainModelCore,
    Windmill,
    Nacelle,
    Rotor,
)
from windmill_pydantic_v1.data_classes._blade import (
    Blade,
    _create_blade_filter,
)
from windmill_pydantic_v1.data_classes._metmast import (
    Metmast,
    _create_metmast_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .blade_query import BladeQueryAPI
    from .metmast_query import MetmastQueryAPI


class WindmillQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_read_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("windmill"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[Windmill], ["*"])]),
                result_cls=Windmill,
                max_retrieve_limit=limit,
            )
        )

    def blades(
        self,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_nacelle: bool = False,
        retrieve_rotor: bool = False,
    ) -> BladeQueryAPI[T_DomainModelList]:
        """Query along the blade edges of the windmill.

        Args:
            is_damaged: The is damaged to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of blade edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_nacelle: Whether to retrieve the nacelle for each windmill or not.
            retrieve_rotor: Whether to retrieve the rotor for each windmill or not.

        Returns:
            BladeQueryAPI: The query API for the blade.
        """
        from .blade_query import BladeQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-models", "Windmill.blades"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("blades"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = self._view_by_read_class[Blade]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_blade_filter(
            view_id,
            is_damaged,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_nacelle:
            self._query_append_nacelle(from_)
        if retrieve_rotor:
            self._query_append_rotor(from_)
        return BladeQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

    def metmast(
        self,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_nacelle: bool = False,
        retrieve_rotor: bool = False,
    ) -> MetmastQueryAPI[T_DomainModelList]:
        """Query along the metmast edges of the windmill.

        Args:
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of metmast edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_nacelle: Whether to retrieve the nacelle for each windmill or not.
            retrieve_rotor: Whether to retrieve the rotor for each windmill or not.

        Returns:
            MetmastQueryAPI: The query API for the metmast.
        """
        from .metmast_query import MetmastQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-models", "Windmill.metmast"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("metmast"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = self._view_by_read_class[Metmast]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_metmast_filter(
            view_id,
            min_position,
            max_position,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_nacelle:
            self._query_append_nacelle(from_)
        if retrieve_rotor:
            self._query_append_rotor(from_)
        return MetmastQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

    def query(
        self,
        retrieve_nacelle: bool = False,
        retrieve_rotor: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_nacelle: Whether to retrieve the nacelle for each windmill or not.
            retrieve_rotor: Whether to retrieve the rotor for each windmill or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_nacelle:
            self._query_append_nacelle(from_)
        if retrieve_rotor:
            self._query_append_rotor(from_)
        return self._query()

    def _query_append_nacelle(self, from_: str) -> None:
        view_id = self._view_by_read_class[Nacelle]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("nacelle"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[Windmill].as_property_ref("nacelle"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Nacelle,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_rotor(self, from_: str) -> None:
        view_id = self._view_by_read_class[Rotor]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("rotor"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[Windmill].as_property_ref("rotor"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Rotor,
                is_single_direct_relation=True,
            ),
        )
