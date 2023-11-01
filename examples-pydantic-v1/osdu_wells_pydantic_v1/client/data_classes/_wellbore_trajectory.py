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
    from ._wellbore_trajectory_data import WellboreTrajectoryDataApply

__all__ = [
    "WellboreTrajectory",
    "WellboreTrajectoryApply",
    "WellboreTrajectoryList",
    "WellboreTrajectoryApplyList",
    "WellboreTrajectoryFields",
    "WellboreTrajectoryTextFields",
]


WellboreTrajectoryTextFields = Literal["create_time", "create_user", "id", "kind", "modify_time", "modify_user"]
WellboreTrajectoryFields = Literal["create_time", "create_user", "id", "kind", "modify_time", "modify_user", "version"]

_WELLBORETRAJECTORY_PROPERTIES_BY_FIELD = {
    "create_time": "createTime",
    "create_user": "createUser",
    "id": "id",
    "kind": "kind",
    "modify_time": "modifyTime",
    "modify_user": "modifyUser",
    "version": "version",
}


class WellboreTrajectory(DomainModel):
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

    def as_apply(self) -> WellboreTrajectoryApply:
        return WellboreTrajectoryApply(
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


class WellboreTrajectoryApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    acl: Union[AclApply, str, None] = Field(None, repr=False)
    ancestry: Union[AncestryApply, str, None] = Field(None, repr=False)
    create_time: Optional[str] = Field(None, alias="createTime")
    create_user: Optional[str] = Field(None, alias="createUser")
    data: Union[WellboreTrajectoryDataApply, str, None] = Field(None, repr=False)
    id: Optional[str] = None
    kind: Optional[str] = None
    legal: Union[LegalApply, str, None] = Field(None, repr=False)
    meta: Union[list[MetaApply], list[str], None] = Field(default=None, repr=False)
    modify_time: Optional[str] = Field(None, alias="modifyTime")
    modify_user: Optional[str] = Field(None, alias="modifyUser")
    tags: Union[TagsApply, str, None] = Field(None, repr=False)
    version: Optional[int] = None

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

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
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "WellboreTrajectory", "5c4afa33e6bd65"),
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
                instances = meta._to_instances_apply(cache, write_view)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.acl, DomainModelApply):
            instances = self.acl._to_instances_apply(cache, write_view)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.ancestry, DomainModelApply):
            instances = self.ancestry._to_instances_apply(cache, write_view)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.data, DomainModelApply):
            instances = self.data._to_instances_apply(cache, write_view)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.legal, DomainModelApply):
            instances = self.legal._to_instances_apply(cache, write_view)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.tags, DomainModelApply):
            instances = self.tags._to_instances_apply(cache, write_view)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_meta_edge(self, meta: Union[str, MetaApply]) -> dm.EdgeApply:
        if isinstance(meta, str):
            end_node_ext_id = meta
        elif isinstance(meta, DomainModelApply):
            end_node_ext_id = meta.external_id
        else:
            raise TypeError(f"Expected str or MetaApply, got {type(meta)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectory.meta"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )


class WellboreTrajectoryList(TypeList[WellboreTrajectory]):
    _NODE = WellboreTrajectory

    def as_apply(self) -> WellboreTrajectoryApplyList:
        return WellboreTrajectoryApplyList([node.as_apply() for node in self.data])


class WellboreTrajectoryApplyList(TypeApplyList[WellboreTrajectoryApply]):
    _NODE = WellboreTrajectoryApply
