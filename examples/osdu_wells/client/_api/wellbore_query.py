from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from osdu_wells.client.data_classes import (
    DomainModelApply,
    Wellbore,
    WellboreApply,
    Acl,
    AclApply,
    Ancestry,
    AncestryApply,
    WellboreData,
    WellboreDataApply,
    Legal,
    LegalApply,
    Tags,
    TagsApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .meta_query import MetaQueryAPI


class WellboreQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("wellbore"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[WellboreApply], ["*"])]),
                result_cls=Wellbore,
                max_retrieve_limit=limit,
            )
        )

    def meta(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_acl: bool = False,
        retrieve_ancestry: bool = False,
        retrieve_data: bool = False,
        retrieve_legal: bool = False,
        retrieve_tags: bool = False,
    ) -> MetaQueryAPI[T_DomainModelList]:
        """Query along the meta edges of the wellbore.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of meta edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_acl: Whether to retrieve the acl for each wellbore or not.
            retrieve_ancestry: Whether to retrieve the ancestry for each wellbore or not.
            retrieve_data: Whether to retrieve the datum for each wellbore or not.
            retrieve_legal: Whether to retrieve the legal for each wellbore or not.
            retrieve_tags: Whether to retrieve the tag for each wellbore or not.

        Returns:
            MetaQueryAPI: The query API for the meta.
        """
        from .meta_query import MetaQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Wellbore.meta"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("meta"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_acl:
            self._query_append_acl(from_)
        if retrieve_ancestry:
            self._query_append_ancestry(from_)
        if retrieve_data:
            self._query_append_data(from_)
        if retrieve_legal:
            self._query_append_legal(from_)
        if retrieve_tags:
            self._query_append_tags(from_)
        return MetaQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
        retrieve_acl: bool = False,
        retrieve_ancestry: bool = False,
        retrieve_data: bool = False,
        retrieve_legal: bool = False,
        retrieve_tags: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_acl: Whether to retrieve the acl for each wellbore or not.
            retrieve_ancestry: Whether to retrieve the ancestry for each wellbore or not.
            retrieve_data: Whether to retrieve the datum for each wellbore or not.
            retrieve_legal: Whether to retrieve the legal for each wellbore or not.
            retrieve_tags: Whether to retrieve the tag for each wellbore or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_acl:
            self._query_append_acl(from_)
        if retrieve_ancestry:
            self._query_append_ancestry(from_)
        if retrieve_data:
            self._query_append_data(from_)
        if retrieve_legal:
            self._query_append_legal(from_)
        if retrieve_tags:
            self._query_append_tags(from_)
        return self._query()

    def _query_append_person(self, from_: str) -> None:
        view_id = self._view_by_write_class[AclApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("acl"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[WellboreApply].as_property_ref("person"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Acl,
            ),
        )

    def _query_append_person(self, from_: str) -> None:
        view_id = self._view_by_write_class[AncestryApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("ancestry"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[WellboreApply].as_property_ref("person"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Ancestry,
            ),
        )

    def _query_append_person(self, from_: str) -> None:
        view_id = self._view_by_write_class[WellboreDataApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("data"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[WellboreApply].as_property_ref("person"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=WellboreData,
            ),
        )

    def _query_append_person(self, from_: str) -> None:
        view_id = self._view_by_write_class[LegalApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("legal"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[WellboreApply].as_property_ref("person"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Legal,
            ),
        )

    def _query_append_person(self, from_: str) -> None:
        view_id = self._view_by_write_class[TagsApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("tags"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[WellboreApply].as_property_ref("person"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Tags,
            ),
        )
