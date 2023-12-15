from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

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
    "Artefacts",
    "ArtefactsApply",
    "ArtefactsList",
    "ArtefactsApplyList",
    "ArtefactsFields",
    "ArtefactsTextFields",
]


ArtefactsTextFields = Literal["resource_id", "resource_kind", "role_id"]
ArtefactsFields = Literal["resource_id", "resource_kind", "role_id"]

_ARTEFACTS_PROPERTIES_BY_FIELD = {
    "resource_id": "ResourceID",
    "resource_kind": "ResourceKind",
    "role_id": "RoleID",
}


class Artefacts(DomainModel):
    """This represents the reading version of artefact.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the artefact.
        resource_id: The resource id field.
        resource_kind: The resource kind field.
        role_id: The role id field.
        created_time: The created time of the artefact node.
        last_updated_time: The last updated time of the artefact node.
        deleted_time: If present, the deleted time of the artefact node.
        version: The version of the artefact node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    resource_id: Optional[str] = Field(None, alias="ResourceID")
    resource_kind: Optional[str] = Field(None, alias="ResourceKind")
    role_id: Optional[str] = Field(None, alias="RoleID")

    def as_apply(self) -> ArtefactsApply:
        """Convert this read version of artefact to the writing version."""
        return ArtefactsApply(
            space=self.space,
            external_id=self.external_id,
            resource_id=self.resource_id,
            resource_kind=self.resource_kind,
            role_id=self.role_id,
        )


class ArtefactsApply(DomainModelApply):
    """This represents the writing version of artefact.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the artefact.
        resource_id: The resource id field.
        resource_kind: The resource kind field.
        role_id: The role id field.
        existing_version: Fail the ingestion request if the artefact version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    resource_id: Optional[str] = Field(None, alias="ResourceID")
    resource_kind: Optional[str] = Field(None, alias="ResourceKind")
    role_id: Optional[str] = Field(None, alias="RoleID")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Artefacts", "7a44a1f4dac367"
        )

        properties = {}
        if self.resource_id is not None:
            properties["ResourceID"] = self.resource_id
        if self.resource_kind is not None:
            properties["ResourceKind"] = self.resource_kind
        if self.role_id is not None:
            properties["RoleID"] = self.role_id

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


class ArtefactsList(DomainModelList[Artefacts]):
    """List of artefacts in the read version."""

    _INSTANCE = Artefacts

    def as_apply(self) -> ArtefactsApplyList:
        """Convert these read versions of artefact to the writing versions."""
        return ArtefactsApplyList([node.as_apply() for node in self.data])


class ArtefactsApplyList(DomainModelApplyList[ArtefactsApply]):
    """List of artefacts in the writing version."""

    _INSTANCE = ArtefactsApply


def _create_artefact_filter(
    view_id: dm.ViewId,
    resource_id: str | list[str] | None = None,
    resource_id_prefix: str | None = None,
    resource_kind: str | list[str] | None = None,
    resource_kind_prefix: str | None = None,
    role_id: str | list[str] | None = None,
    role_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if resource_id is not None and isinstance(resource_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ResourceID"), value=resource_id))
    if resource_id and isinstance(resource_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ResourceID"), values=resource_id))
    if resource_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ResourceID"), value=resource_id_prefix))
    if resource_kind is not None and isinstance(resource_kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ResourceKind"), value=resource_kind))
    if resource_kind and isinstance(resource_kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ResourceKind"), values=resource_kind))
    if resource_kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ResourceKind"), value=resource_kind_prefix))
    if role_id is not None and isinstance(role_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("RoleID"), value=role_id))
    if role_id and isinstance(role_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("RoleID"), values=role_id))
    if role_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("RoleID"), value=role_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
