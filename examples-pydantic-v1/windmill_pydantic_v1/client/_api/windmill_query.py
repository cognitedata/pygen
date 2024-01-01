from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from windmill_pydantic_v1.client.data_classes import (
    DomainModelCore,
    Windmill,
    Nacelle,
    NacelleApply,
    Rotor,
    RotorApply,
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
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_nacelle: bool = False,
        retrieve_rotor: bool = False,
    ) -> BladeQueryAPI[T_DomainModelList]:
        """Query along the blade edges of the windmill.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of blade edges to return. Defaults to 25. Set to -1, float("inf") or None
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
            external_id_prefix=external_id_prefix,
            space=space,
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
        if retrieve_nacelle:
            self._query_append_nacelle(from_)
        if retrieve_rotor:
            self._query_append_rotor(from_)
        return BladeQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def metmast(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_nacelle: bool = False,
        retrieve_rotor: bool = False,
    ) -> MetmastQueryAPI[T_DomainModelList]:
        """Query along the metmast edges of the windmill.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metmast edges to return. Defaults to 25. Set to -1, float("inf") or None
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
            external_id_prefix=external_id_prefix,
            space=space,
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
        if retrieve_nacelle:
            self._query_append_nacelle(from_)
        if retrieve_rotor:
            self._query_append_rotor(from_)
        return MetmastQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

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
            ),
        )
