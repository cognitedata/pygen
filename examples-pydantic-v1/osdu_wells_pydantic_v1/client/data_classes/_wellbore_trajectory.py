from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

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

if TYPE_CHECKING:
    from ._acl import Acl, AclApply
    from ._ancestry import Ancestry, AncestryApply
    from ._legal import Legal, LegalApply
    from ._meta import Meta, MetaApply
    from ._tags import Tags, TagsApply
    from ._wellbore_trajectory_data import WellboreTrajectoryData, WellboreTrajectoryDataApply


__all__ = [
    "WellboreTrajectory",
    "WellboreTrajectoryApply",
    "WellboreTrajectoryList",
    "WellboreTrajectoryApplyList",
    "WellboreTrajectoryFields",
    "WellboreTrajectoryTextFields",
]


WellboreTrajectoryTextFields = Literal["create_time", "create_user", "id_", "kind", "modify_time", "modify_user"]
WellboreTrajectoryFields = Literal[
    "create_time", "create_user", "id_", "kind", "modify_time", "modify_user", "version_"
]

_WELLBORETRAJECTORY_PROPERTIES_BY_FIELD = {
    "create_time": "createTime",
    "create_user": "createUser",
    "id_": "id",
    "kind": "kind",
    "modify_time": "modifyTime",
    "modify_user": "modifyUser",
    "version_": "version",
}


class WellboreTrajectory(DomainModel):
    """This represents the reading version of wellbore trajectory.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wellbore trajectory.
        acl: The acl field.
        ancestry: The ancestry field.
        create_time: The create time field.
        create_user: The create user field.
        data: The datum field.
        id_: The id field.
        kind: The kind field.
        legal: The legal field.
        meta: The meta field.
        modify_time: The modify time field.
        modify_user: The modify user field.
        tags: The tag field.
        version_: The version field.
        created_time: The created time of the wellbore trajectory node.
        last_updated_time: The last updated time of the wellbore trajectory node.
        deleted_time: If present, the deleted time of the wellbore trajectory node.
        version: The version of the wellbore trajectory node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    acl: Union[Acl, str, dm.NodeId, None] = Field(None, repr=False)
    ancestry: Union[Ancestry, str, dm.NodeId, None] = Field(None, repr=False)
    create_time: Optional[str] = Field(None, alias="createTime")
    create_user: Optional[str] = Field(None, alias="createUser")
    data: Union[WellboreTrajectoryData, str, dm.NodeId, None] = Field(None, repr=False)
    id_: Optional[str] = Field(None, alias="id")
    kind: Optional[str] = None
    legal: Union[Legal, str, dm.NodeId, None] = Field(None, repr=False)
    meta: Union[list[Meta], list[str], None] = Field(default=None, repr=False)
    modify_time: Optional[str] = Field(None, alias="modifyTime")
    modify_user: Optional[str] = Field(None, alias="modifyUser")
    tags: Union[Tags, str, dm.NodeId, None] = Field(None, repr=False)
    version_: Optional[int] = Field(None, alias="version")

    def as_apply(self) -> WellboreTrajectoryApply:
        """Convert this read version of wellbore trajectory to the writing version."""
        return WellboreTrajectoryApply(
            space=self.space,
            external_id=self.external_id,
            acl=self.acl.as_apply() if isinstance(self.acl, DomainModel) else self.acl,
            ancestry=self.ancestry.as_apply() if isinstance(self.ancestry, DomainModel) else self.ancestry,
            create_time=self.create_time,
            create_user=self.create_user,
            data=self.data.as_apply() if isinstance(self.data, DomainModel) else self.data,
            id_=self.id_,
            kind=self.kind,
            legal=self.legal.as_apply() if isinstance(self.legal, DomainModel) else self.legal,
            meta=[meta.as_apply() if isinstance(meta, DomainModel) else meta for meta in self.meta or []],
            modify_time=self.modify_time,
            modify_user=self.modify_user,
            tags=self.tags.as_apply() if isinstance(self.tags, DomainModel) else self.tags,
            version_=self.version_,
        )


class WellboreTrajectoryApply(DomainModelApply):
    """This represents the writing version of wellbore trajectory.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wellbore trajectory.
        acl: The acl field.
        ancestry: The ancestry field.
        create_time: The create time field.
        create_user: The create user field.
        data: The datum field.
        id_: The id field.
        kind: The kind field.
        legal: The legal field.
        meta: The meta field.
        modify_time: The modify time field.
        modify_user: The modify user field.
        tags: The tag field.
        version_: The version field.
        existing_version: Fail the ingestion request if the wellbore trajectory version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    acl: Union[AclApply, str, dm.NodeId, None] = Field(None, repr=False)
    ancestry: Union[AncestryApply, str, dm.NodeId, None] = Field(None, repr=False)
    create_time: Optional[str] = Field(None, alias="createTime")
    create_user: Optional[str] = Field(None, alias="createUser")
    data: Union[WellboreTrajectoryDataApply, str, dm.NodeId, None] = Field(None, repr=False)
    id_: Optional[str] = Field(None, alias="id")
    kind: Optional[str] = None
    legal: Union[LegalApply, str, dm.NodeId, None] = Field(None, repr=False)
    meta: Union[list[MetaApply], list[str], None] = Field(default=None, repr=False)
    modify_time: Optional[str] = Field(None, alias="modifyTime")
    modify_user: Optional[str] = Field(None, alias="modifyUser")
    tags: Union[TagsApply, str, dm.NodeId, None] = Field(None, repr=False)
    version_: Optional[int] = Field(None, alias="version")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "WellboreTrajectory", "5c4afa33e6bd65"
        )

        properties = {}
        if self.acl is not None:
            properties["acl"] = {
                "space": self.space if isinstance(self.acl, str) else self.acl.space,
                "externalId": self.acl if isinstance(self.acl, str) else self.acl.external_id,
            }
        if self.ancestry is not None:
            properties["ancestry"] = {
                "space": self.space if isinstance(self.ancestry, str) else self.ancestry.space,
                "externalId": self.ancestry if isinstance(self.ancestry, str) else self.ancestry.external_id,
            }
        if self.create_time is not None:
            properties["createTime"] = self.create_time
        if self.create_user is not None:
            properties["createUser"] = self.create_user
        if self.data is not None:
            properties["data"] = {
                "space": self.space if isinstance(self.data, str) else self.data.space,
                "externalId": self.data if isinstance(self.data, str) else self.data.external_id,
            }
        if self.id_ is not None:
            properties["id"] = self.id_
        if self.kind is not None:
            properties["kind"] = self.kind
        if self.legal is not None:
            properties["legal"] = {
                "space": self.space if isinstance(self.legal, str) else self.legal.space,
                "externalId": self.legal if isinstance(self.legal, str) else self.legal.external_id,
            }
        if self.modify_time is not None:
            properties["modifyTime"] = self.modify_time
        if self.modify_user is not None:
            properties["modifyUser"] = self.modify_user
        if self.tags is not None:
            properties["tags"] = {
                "space": self.space if isinstance(self.tags, str) else self.tags.space,
                "externalId": self.tags if isinstance(self.tags, str) else self.tags.external_id,
            }
        if self.version_ is not None:
            properties["version"] = self.version_

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

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectory.meta")
        for meta in self.meta or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=meta, edge_type=edge_type, view_by_write_class=view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.acl, DomainModelApply):
            other_resources = self.acl._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.ancestry, DomainModelApply):
            other_resources = self.ancestry._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.data, DomainModelApply):
            other_resources = self.data._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.legal, DomainModelApply):
            other_resources = self.legal._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.tags, DomainModelApply):
            other_resources = self.tags._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class WellboreTrajectoryList(DomainModelList[WellboreTrajectory]):
    """List of wellbore trajectories in the read version."""

    _INSTANCE = WellboreTrajectory

    def as_apply(self) -> WellboreTrajectoryApplyList:
        """Convert these read versions of wellbore trajectory to the writing versions."""
        return WellboreTrajectoryApplyList([node.as_apply() for node in self.data])


class WellboreTrajectoryApplyList(DomainModelApplyList[WellboreTrajectoryApply]):
    """List of wellbore trajectories in the writing version."""

    _INSTANCE = WellboreTrajectoryApply


def _create_wellbore_trajectory_filter(
    view_id: dm.ViewId,
    acl: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    ancestry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    create_time: str | list[str] | None = None,
    create_time_prefix: str | None = None,
    create_user: str | list[str] | None = None,
    create_user_prefix: str | None = None,
    data: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    id_: str | list[str] | None = None,
    id_prefix: str | None = None,
    kind: str | list[str] | None = None,
    kind_prefix: str | None = None,
    legal: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    modify_time: str | list[str] | None = None,
    modify_time_prefix: str | None = None,
    modify_user: str | list[str] | None = None,
    modify_user_prefix: str | None = None,
    tags: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_version_: int | None = None,
    max_version_: int | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if acl and isinstance(acl, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("acl"), value={"space": "IntegrationTestsImmutable", "externalId": acl}
            )
        )
    if acl and isinstance(acl, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("acl"), value={"space": acl[0], "externalId": acl[1]}))
    if acl and isinstance(acl, list) and isinstance(acl[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("acl"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in acl],
            )
        )
    if acl and isinstance(acl, list) and isinstance(acl[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("acl"), values=[{"space": item[0], "externalId": item[1]} for item in acl]
            )
        )
    if ancestry and isinstance(ancestry, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("ancestry"),
                value={"space": "IntegrationTestsImmutable", "externalId": ancestry},
            )
        )
    if ancestry and isinstance(ancestry, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("ancestry"), value={"space": ancestry[0], "externalId": ancestry[1]}
            )
        )
    if ancestry and isinstance(ancestry, list) and isinstance(ancestry[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("ancestry"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in ancestry],
            )
        )
    if ancestry and isinstance(ancestry, list) and isinstance(ancestry[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("ancestry"),
                values=[{"space": item[0], "externalId": item[1]} for item in ancestry],
            )
        )
    if create_time is not None and isinstance(create_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("createTime"), value=create_time))
    if create_time and isinstance(create_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("createTime"), values=create_time))
    if create_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("createTime"), value=create_time_prefix))
    if create_user is not None and isinstance(create_user, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("createUser"), value=create_user))
    if create_user and isinstance(create_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("createUser"), values=create_user))
    if create_user_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("createUser"), value=create_user_prefix))
    if data and isinstance(data, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("data"), value={"space": "IntegrationTestsImmutable", "externalId": data}
            )
        )
    if data and isinstance(data, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("data"), value={"space": data[0], "externalId": data[1]})
        )
    if data and isinstance(data, list) and isinstance(data[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("data"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in data],
            )
        )
    if data and isinstance(data, list) and isinstance(data[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("data"), values=[{"space": item[0], "externalId": item[1]} for item in data]
            )
        )
    if id_ is not None and isinstance(id_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("id"), value=id_))
    if id_ and isinstance(id_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("id"), values=id_))
    if id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("id"), value=id_prefix))
    if kind is not None and isinstance(kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("kind"), value=kind))
    if kind and isinstance(kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("kind"), values=kind))
    if kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("kind"), value=kind_prefix))
    if legal and isinstance(legal, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("legal"), value={"space": "IntegrationTestsImmutable", "externalId": legal}
            )
        )
    if legal and isinstance(legal, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("legal"), value={"space": legal[0], "externalId": legal[1]})
        )
    if legal and isinstance(legal, list) and isinstance(legal[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("legal"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in legal],
            )
        )
    if legal and isinstance(legal, list) and isinstance(legal[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("legal"), values=[{"space": item[0], "externalId": item[1]} for item in legal]
            )
        )
    if modify_time is not None and isinstance(modify_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("modifyTime"), value=modify_time))
    if modify_time and isinstance(modify_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("modifyTime"), values=modify_time))
    if modify_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("modifyTime"), value=modify_time_prefix))
    if modify_user is not None and isinstance(modify_user, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("modifyUser"), value=modify_user))
    if modify_user and isinstance(modify_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("modifyUser"), values=modify_user))
    if modify_user_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("modifyUser"), value=modify_user_prefix))
    if tags and isinstance(tags, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("tags"), value={"space": "IntegrationTestsImmutable", "externalId": tags}
            )
        )
    if tags and isinstance(tags, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("tags"), value={"space": tags[0], "externalId": tags[1]})
        )
    if tags and isinstance(tags, list) and isinstance(tags[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("tags"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in tags],
            )
        )
    if tags and isinstance(tags, list) and isinstance(tags[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("tags"), values=[{"space": item[0], "externalId": item[1]} for item in tags]
            )
        )
    if min_version_ or max_version_:
        filters.append(dm.filters.Range(view_id.as_property_ref("version"), gte=min_version_, lte=max_version_))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
