from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = ["Acl", "AclApply", "AclList", "AclApplyList", "AclFields", "AclTextFields"]


AclTextFields = Literal["owners", "viewers"]
AclFields = Literal["owners", "viewers"]

_ACL_PROPERTIES_BY_FIELD = {
    "owners": "owners",
    "viewers": "viewers",
}


class Acl(DomainModel):
    """This represents the reading version of acl.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the acl.
        owners: The owner field.
        viewers: The viewer field.
        created_time: The created time of the acl node.
        last_updated_time: The last updated time of the acl node.
        deleted_time: If present, the deleted time of the acl node.
        version: The version of the acl node.
    """

    space: str = "IntegrationTestsImmutable"
    owners: Optional[list[str]] = None
    viewers: Optional[list[str]] = None

    def as_apply(self) -> AclApply:
        """Convert this read version of acl to the writing version."""
        return AclApply(
            space=self.space,
            external_id=self.external_id,
            owners=self.owners,
            viewers=self.viewers,
        )


class AclApply(DomainModelApply):
    """This represents the writing version of acl.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the acl.
        owners: The owner field.
        viewers: The viewer field.
        existing_version: Fail the ingestion request if the acl version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    owners: Optional[list[str]] = None
    viewers: Optional[list[str]] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Acl", "1c4f4a5942a9a8"
        )

        properties = {}
        if self.owners is not None:
            properties["owners"] = self.owners
        if self.viewers is not None:
            properties["viewers"] = self.viewers

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


class AclList(DomainModelList[Acl]):
    """List of acls in the read version."""

    _INSTANCE = Acl

    def as_apply(self) -> AclApplyList:
        """Convert these read versions of acl to the writing versions."""
        return AclApplyList([node.as_apply() for node in self.data])


class AclApplyList(DomainModelApplyList[AclApply]):
    """List of acls in the writing version."""

    _INSTANCE = AclApply


def _create_acl_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
