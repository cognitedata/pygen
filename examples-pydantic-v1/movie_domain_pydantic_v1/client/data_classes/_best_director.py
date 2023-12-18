from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = [
    "BestDirector",
    "BestDirectorApply",
    "BestDirectorList",
    "BestDirectorApplyList",
    "BestDirectorFields",
    "BestDirectorTextFields",
]


BestDirectorTextFields = Literal["name"]
BestDirectorFields = Literal["name", "year"]

_BESTDIRECTOR_PROPERTIES_BY_FIELD = {
    "name": "name",
    "year": "year",
}


class BestDirector(DomainModel):
    """This represents the reading version of best director.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the best director.
        name: The name field.
        year: The year field.
        created_time: The created time of the best director node.
        last_updated_time: The last updated time of the best director node.
        deleted_time: If present, the deleted time of the best director node.
        version: The version of the best director node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: Optional[str] = None
    year: Optional[int] = None

    def as_apply(self) -> BestDirectorApply:
        """Convert this read version of best director to the writing version."""
        return BestDirectorApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            year=self.year,
        )


class BestDirectorApply(DomainModelApply):
    """This represents the writing version of best director.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the best director.
        name: The name field.
        year: The year field.
        existing_version: Fail the ingestion request if the best director version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: str
    year: int

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "BestDirector", "2"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.year is not None:
            properties["year"] = self.year

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("IntegrationTestsImmutable", "BestDirector"),
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class BestDirectorList(DomainModelList[BestDirector]):
    """List of best directors in the read version."""

    _INSTANCE = BestDirector

    def as_apply(self) -> BestDirectorApplyList:
        """Convert these read versions of best director to the writing versions."""
        return BestDirectorApplyList([node.as_apply() for node in self.data])


class BestDirectorApplyList(DomainModelApplyList[BestDirectorApply]):
    """List of best directors in the writing version."""

    _INSTANCE = BestDirectorApply


def _create_best_director_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_year: int | None = None,
    max_year: int | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if min_year or max_year:
        filters.append(dm.filters.Range(view_id.as_property_ref("year"), gte=min_year, lte=max_year))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
