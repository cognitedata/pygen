from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Tags", "TagsApply", "TagsList", "TagsApplyList", "TagsFields", "TagsTextFields"]


TagsTextFields = Literal["name_of_key"]
TagsFields = Literal["name_of_key"]

_TAGS_PROPERTIES_BY_FIELD = {
    "name_of_key": "NameOfKey",
}


class Tags(DomainModel):
    """This represent a read version of tag.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the tag.
        name_of_key: The name of key field.
        created_time: The created time of the tag node.
        last_updated_time: The last updated time of the tag node.
        deleted_time: If present, the deleted time of the tag node.
        version: The version of the tag node.
    """

    space: str = "IntegrationTestsImmutable"
    name_of_key: Optional[str] = Field(None, alias="NameOfKey")

    def as_apply(self) -> TagsApply:
        """Convert this read version of tag to a write version."""
        return TagsApply(
            space=self.space,
            external_id=self.external_id,
            name_of_key=self.name_of_key,
        )


class TagsApply(DomainModelApply):
    """This represent a write version of tag.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the tag.
        name_of_key: The name of key field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    name_of_key: Optional[str] = Field(None, alias="NameOfKey")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.name_of_key is not None:
            properties["NameOfKey"] = self.name_of_key
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Tags", "77ace80e524925"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class TagsList(TypeList[Tags]):
    """List of tags in read version."""

    _NODE = Tags

    def as_apply(self) -> TagsApplyList:
        """Convert this read version of tag to a write version."""
        return TagsApplyList([node.as_apply() for node in self.data])


class TagsApplyList(TypeApplyList[TagsApply]):
    """List of tags in write version."""

    _NODE = TagsApply
