from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = ["Tags", "TagsApply", "TagsList", "TagsApplyList", "TagsFields", "TagsTextFields"]


TagsTextFields = Literal["name_of_key"]
TagsFields = Literal["name_of_key"]

_TAGS_PROPERTIES_BY_FIELD = {
    "name_of_key": "NameOfKey",
}


class Tags(DomainModel):
    """This represents the reading version of tag.

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
        """Convert this read version of tag to the writing version."""
        return TagsApply(
            space=self.space,
            external_id=self.external_id,
            name_of_key=self.name_of_key,
        )


class TagsApply(DomainModelApply):
    """This represents the writing version of tag.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the tag.
        name_of_key: The name of key field.
        existing_version: Fail the ingestion request if the tag version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    name_of_key: Optional[str] = Field(None, alias="NameOfKey")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Tags", "77ace80e524925"
        )

        properties = {}
        if self.name_of_key is not None:
            properties["NameOfKey"] = self.name_of_key

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


class TagsList(DomainModelList[Tags]):
    """List of tags in the read version."""

    _INSTANCE = Tags

    def as_apply(self) -> TagsApplyList:
        """Convert these read versions of tag to the writing versions."""
        return TagsApplyList([node.as_apply() for node in self.data])


class TagsApplyList(DomainModelApplyList[TagsApply]):
    """List of tags in the writing version."""

    _INSTANCE = TagsApply


def _create_tag_filter(
    view_id: dm.ViewId,
    name_of_key: str | list[str] | None = None,
    name_of_key_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name_of_key and isinstance(name_of_key, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("NameOfKey"), value=name_of_key))
    if name_of_key and isinstance(name_of_key, list):
        filters.append(dm.filters.In(view_id.as_property_ref("NameOfKey"), values=name_of_key))
    if name_of_key_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("NameOfKey"), value=name_of_key_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
