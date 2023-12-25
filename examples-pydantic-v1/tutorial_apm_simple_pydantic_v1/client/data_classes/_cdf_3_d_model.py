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
    from ._cdf_3_d_connection_properties import Cdf3dConnectionProperties, Cdf3dConnectionPropertiesApply


__all__ = [
    "Cdf3dModel",
    "Cdf3dModelApply",
    "Cdf3dModelList",
    "Cdf3dModelApplyList",
    "Cdf3dModelFields",
    "Cdf3dModelTextFields",
]


Cdf3dModelTextFields = Literal["name"]
Cdf3dModelFields = Literal["name"]

_CDF3DMODEL_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class Cdf3dModel(DomainModel):
    """This represents the reading version of cdf 3 d model.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf 3 d model.
        entities: Collection of Cdf3dEntity that are part of this Cdf3dModel
        name: The name field.
        created_time: The created time of the cdf 3 d model node.
        last_updated_time: The last updated time of the cdf 3 d model node.
        deleted_time: If present, the deleted time of the cdf 3 d model node.
        version: The version of the cdf 3 d model node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    entities: Optional[list[Cdf3dConnectionProperties]] = Field(default=None, repr=False)
    name: Optional[str] = None

    def as_apply(self) -> Cdf3dModelApply:
        """Convert this read version of cdf 3 d model to the writing version."""
        return Cdf3dModelApply(
            space=self.space,
            external_id=self.external_id,
            entities=[entity.as_apply() for entity in self.entities or []],
            name=self.name,
        )


class Cdf3dModelApply(DomainModelApply):
    """This represents the writing version of cdf 3 d model.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf 3 d model.
        entities: Collection of Cdf3dEntity that are part of this Cdf3dModel
        name: The name field.
        existing_version: Fail the ingestion request if the cdf 3 d model version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    entities: Optional[list[Cdf3dConnectionPropertiesApply]] = Field(default=None, repr=False)
    name: str

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "cdf_3d_schema", "Cdf3dModel", "1"
        )

        properties = {}

        if self.name is not None:
            properties["name"] = self.name

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

        for entity in self.entities or []:
            if isinstance(entity, DomainRelationApply):
                other_resources = entity._to_instances_apply(
                    cache,
                    self,
                    dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
                    view_by_write_class,
                )
                resources.extend(other_resources)

        return resources


class Cdf3dModelList(DomainModelList[Cdf3dModel]):
    """List of cdf 3 d models in the read version."""

    _INSTANCE = Cdf3dModel

    def as_apply(self) -> Cdf3dModelApplyList:
        """Convert these read versions of cdf 3 d model to the writing versions."""
        return Cdf3dModelApplyList([node.as_apply() for node in self.data])


class Cdf3dModelApplyList(DomainModelApplyList[Cdf3dModelApply]):
    """List of cdf 3 d models in the writing version."""

    _INSTANCE = Cdf3dModelApply


def _create_cdf_3_d_model_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
