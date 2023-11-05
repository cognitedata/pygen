from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._acl import AclApply
    from ._ancestry import AncestryApply
    from ._legal import LegalApply
    from ._meta import MetaApply
    from ._tags import TagsApply
    from ._wellbore_data import WellboreDataApply

__all__ = ["Wellbore", "WellboreApply", "WellboreList", "WellboreApplyList", "WellboreFields", "WellboreTextFields"]


WellboreTextFields = Literal["create_time", "create_user", "id", "kind", "modify_time", "modify_user"]
WellboreFields = Literal["create_time", "create_user", "id", "kind", "modify_time", "modify_user", "version"]

_WELLBORE_PROPERTIES_BY_FIELD = {
    "create_time": "createTime",
    "create_user": "createUser",
    "id": "id",
    "kind": "kind",
    "modify_time": "modifyTime",
    "modify_user": "modifyUser",
    "version": "version",
}


class Wellbore(DomainModel):
    """This represent a read version of wellbore.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wellbore.
        acl: The acl field.
        ancestry: The ancestry field.
        create_time: The create time field.
        create_user: The create user field.
        data: The datum field.
        id: The id field.
        kind: The kind field.
        legal: The legal field.
        meta: The meta field.
        modify_time: The modify time field.
        modify_user: The modify user field.
        tags: The tag field.
        version: The version field.
        created_time: The created time of the wellbore node.
        last_updated_time: The last updated time of the wellbore node.
        deleted_time: If present, the deleted time of the wellbore node.
        version: The version of the wellbore node.
    """

    space: str = "IntegrationTestsImmutable"
    acl: Optional[str] = None
    ancestry: Optional[str] = None
    create_time: Optional[str] = Field(None, alias="createTime")
    create_user: Optional[str] = Field(None, alias="createUser")
    data: Optional[str] = None
    id: Optional[str] = None
    kind: Optional[str] = None
    legal: Optional[str] = None
    meta: Optional[list[str]] = None
    modify_time: Optional[str] = Field(None, alias="modifyTime")
    modify_user: Optional[str] = Field(None, alias="modifyUser")
    tags: Optional[str] = None
    version: Optional[int] = None

    def as_apply(self) -> WellboreApply:
        """Convert this read version of wellbore to a write version."""
        return WellboreApply(
            space=self.space,
            external_id=self.external_id,
            acl=self.acl,
            ancestry=self.ancestry,
            create_time=self.create_time,
            create_user=self.create_user,
            data=self.data,
            id=self.id,
            kind=self.kind,
            legal=self.legal,
            meta=self.meta,
            modify_time=self.modify_time,
            modify_user=self.modify_user,
            tags=self.tags,
            version=self.version,
        )


class WellboreApply(DomainModelApply):
    """This represent a write version of wellbore.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wellbore.
        acl: The acl field.
        ancestry: The ancestry field.
        create_time: The create time field.
        create_user: The create user field.
        data: The datum field.
        id: The id field.
        kind: The kind field.
        legal: The legal field.
        meta: The meta field.
        modify_time: The modify time field.
        modify_user: The modify user field.
        tags: The tag field.
        version: The version field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    acl: Union[AclApply, str, None] = Field(None, repr=False)
    ancestry: Union[AncestryApply, str, None] = Field(None, repr=False)
    create_time: Optional[str] = Field(None, alias="createTime")
    create_user: Optional[str] = Field(None, alias="createUser")
    data: Union[WellboreDataApply, str, None] = Field(None, repr=False)
    id: Optional[str] = None
    kind: Optional[str] = None
    legal: Union[LegalApply, str, None] = Field(None, repr=False)
    meta: Union[list[MetaApply], list[str], None] = Field(default=None, repr=False)
    modify_time: Optional[str] = Field(None, alias="modifyTime")
    modify_user: Optional[str] = Field(None, alias="modifyUser")
    tags: Union[TagsApply, str, None] = Field(None, repr=False)
    version: Optional[int] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.acl is not None:
            properties["acl"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.acl if isinstance(self.acl, str) else self.acl.external_id,
            }
        if self.ancestry is not None:
            properties["ancestry"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.ancestry if isinstance(self.ancestry, str) else self.ancestry.external_id,
            }
        if self.create_time is not None:
            properties["createTime"] = self.create_time
        if self.create_user is not None:
            properties["createUser"] = self.create_user
        if self.data is not None:
            properties["data"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.data if isinstance(self.data, str) else self.data.external_id,
            }
        if self.id is not None:
            properties["id"] = self.id
        if self.kind is not None:
            properties["kind"] = self.kind
        if self.legal is not None:
            properties["legal"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.legal if isinstance(self.legal, str) else self.legal.external_id,
            }
        if self.modify_time is not None:
            properties["modifyTime"] = self.modify_time
        if self.modify_user is not None:
            properties["modifyUser"] = self.modify_user
        if self.tags is not None:
            properties["tags"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.tags if isinstance(self.tags, str) else self.tags.external_id,
            }
        if self.version is not None:
            properties["version"] = self.version
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Wellbore", "7a44cf38aa4fe7"),
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

        for meta in self.meta or []:
            edge = self._create_meta_edge(meta)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(meta, DomainModelApply):
                instances = meta._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.acl, DomainModelApply):
            instances = self.acl._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.ancestry, DomainModelApply):
            instances = self.ancestry._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.data, DomainModelApply):
            instances = self.data._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.legal, DomainModelApply):
            instances = self.legal._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.tags, DomainModelApply):
            instances = self.tags._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_meta_edge(self, meta: Union[str, MetaApply]) -> dm.EdgeApply:
        if isinstance(meta, str):
            end_space, end_node_ext_id = self.space, meta
        elif isinstance(meta, DomainModelApply):
            end_space, end_node_ext_id = meta.space, meta.external_id
        else:
            raise TypeError(f"Expected str or MetaApply, got {type(meta)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Wellbore.meta"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )


class WellboreList(TypeList[Wellbore]):
    """List of wellbores in read version."""

    _NODE = Wellbore

    def as_apply(self) -> WellboreApplyList:
        """Convert this read version of wellbore to a write version."""
        return WellboreApplyList([node.as_apply() for node in self.data])


class WellboreApplyList(TypeApplyList[WellboreApply]):
    """List of wellbores in write version."""

    _NODE = WellboreApply
