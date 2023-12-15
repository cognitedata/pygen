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


__all__ = ["Ancestry", "AncestryApply", "AncestryList", "AncestryApplyList", "AncestryFields", "AncestryTextFields"]


AncestryTextFields = Literal["parents"]
AncestryFields = Literal["parents"]

_ANCESTRY_PROPERTIES_BY_FIELD = {
    "parents": "parents",
}


class Ancestry(DomainModel):
    """This represents the reading version of ancestry.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the ancestry.
        parents: The parent field.
        created_time: The created time of the ancestry node.
        last_updated_time: The last updated time of the ancestry node.
        deleted_time: If present, the deleted time of the ancestry node.
        version: The version of the ancestry node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    parents: Optional[list[str]] = None

    def as_apply(self) -> AncestryApply:
        """Convert this read version of ancestry to the writing version."""
        return AncestryApply(
            space=self.space,
            external_id=self.external_id,
            parents=self.parents,
        )


class AncestryApply(DomainModelApply):
    """This represents the writing version of ancestry.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the ancestry.
        parents: The parent field.
        existing_version: Fail the ingestion request if the ancestry version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    parents: Optional[list[str]] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Ancestry", "624b46e28cdd69"
        )

        properties = {}
        if self.parents is not None:
            properties["parents"] = self.parents

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
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


class AncestryList(DomainModelList[Ancestry]):
    """List of ancestries in the read version."""

    _INSTANCE = Ancestry

    def as_apply(self) -> AncestryApplyList:
        """Convert these read versions of ancestry to the writing versions."""
        return AncestryApplyList([node.as_apply() for node in self.data])


class AncestryApplyList(DomainModelApplyList[AncestryApply]):
    """List of ancestries in the writing version."""

    _INSTANCE = AncestryApply


def _create_ancestry_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
