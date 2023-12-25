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


__all__ = ["Cdf3dEntity", "Cdf3dEntityApply", "Cdf3dEntityList", "Cdf3dEntityApplyList"]


class Cdf3dEntity(DomainModel):
    """This represents the reading version of cdf 3 d entity.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf 3 d entity.
        in_model_3_d: Cdf3dModel the Cdf3dEntity is part of
        created_time: The created time of the cdf 3 d entity node.
        last_updated_time: The last updated time of the cdf 3 d entity node.
        deleted_time: If present, the deleted time of the cdf 3 d entity node.
        version: The version of the cdf 3 d entity node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    in_model_3_d: Optional[list[Cdf3dConnectionProperties]] = Field(default=None, repr=False, alias="inModel3d")

    def as_apply(self) -> Cdf3dEntityApply:
        """Convert this read version of cdf 3 d entity to the writing version."""
        return Cdf3dEntityApply(
            space=self.space,
            external_id=self.external_id,
            in_model_3_d=[in_model_3_d.as_apply() for in_model_3_d in self.in_model_3_d or []],
        )


class Cdf3dEntityApply(DomainModelApply):
    """This represents the writing version of cdf 3 d entity.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf 3 d entity.
        in_model_3_d: Cdf3dModel the Cdf3dEntity is part of
        existing_version: Fail the ingestion request if the cdf 3 d entity version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    in_model_3_d: Optional[list[Cdf3dConnectionPropertiesApply]] = Field(default=None, repr=False, alias="inModel3d")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources
        cache.add(self.as_tuple_id())

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "cdf_3d_schema", "Cdf3dEntity", "1"
        )

        for in_model_3_d in self.in_model_3_d or []:
            if isinstance(in_model_3_d, DomainRelationApply):
                other_resources = in_model_3_d._to_instances_apply(
                    cache,
                    self,
                    dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
                    view_by_write_class,
                )
                resources.extend(other_resources)

        return resources


class Cdf3dEntityList(DomainModelList[Cdf3dEntity]):
    """List of cdf 3 d entities in the read version."""

    _INSTANCE = Cdf3dEntity

    def as_apply(self) -> Cdf3dEntityApplyList:
        """Convert these read versions of cdf 3 d entity to the writing versions."""
        return Cdf3dEntityApplyList([node.as_apply() for node in self.data])


class Cdf3dEntityApplyList(DomainModelApplyList[Cdf3dEntityApply]):
    """List of cdf 3 d entities in the writing version."""

    _INSTANCE = Cdf3dEntityApply


def _create_cdf_3_d_entity_filter(
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
