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
    "SubInterface",
    "SubInterfaceApply",
    "SubInterfaceList",
    "SubInterfaceApplyList",
    "SubInterfaceFields",
    "SubInterfaceTextFields",
]


SubInterfaceTextFields = Literal["main_value", "sub_value"]
SubInterfaceFields = Literal["main_value", "sub_value"]

_SUBINTERFACE_PROPERTIES_BY_FIELD = {
    "main_value": "mainValue",
    "sub_value": "subValue",
}


class SubInterface(DomainModel):
    """This represents the reading version of sub interface.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sub interface.
        main_value: The main value field.
        sub_value: The sub value field.
        created_time: The created time of the sub interface node.
        last_updated_time: The last updated time of the sub interface node.
        deleted_time: If present, the deleted time of the sub interface node.
        version: The version of the sub interface node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    main_value: Optional[str] = Field(None, alias="mainValue")
    sub_value: Optional[str] = Field(None, alias="subValue")

    def as_apply(self) -> SubInterfaceApply:
        """Convert this read version of sub interface to the writing version."""
        return SubInterfaceApply(
            space=self.space,
            external_id=self.external_id,
            main_value=self.main_value,
            sub_value=self.sub_value,
        )


class SubInterfaceApply(DomainModelApply):
    """This represents the writing version of sub interface.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sub interface.
        main_value: The main value field.
        sub_value: The sub value field.
        existing_version: Fail the ingestion request if the sub interface version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    main_value: Optional[str] = Field(None, alias="mainValue")
    sub_value: Optional[str] = Field(None, alias="subValue")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "pygen-models", "SubInterface", "1"
        )

        properties = {}

        if self.main_value is not None:
            properties["mainValue"] = self.main_value

        if self.sub_value is not None:
            properties["subValue"] = self.sub_value

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


class SubInterfaceList(DomainModelList[SubInterface]):
    """List of sub interfaces in the read version."""

    _INSTANCE = SubInterface

    def as_apply(self) -> SubInterfaceApplyList:
        """Convert these read versions of sub interface to the writing versions."""
        return SubInterfaceApplyList([node.as_apply() for node in self.data])


class SubInterfaceApplyList(DomainModelApplyList[SubInterfaceApply]):
    """List of sub interfaces in the writing version."""

    _INSTANCE = SubInterfaceApply


def _create_sub_interface_filter(
    view_id: dm.ViewId,
    main_value: str | list[str] | None = None,
    main_value_prefix: str | None = None,
    sub_value: str | list[str] | None = None,
    sub_value_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if main_value is not None and isinstance(main_value, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("mainValue"), value=main_value))
    if main_value and isinstance(main_value, list):
        filters.append(dm.filters.In(view_id.as_property_ref("mainValue"), values=main_value))
    if main_value_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("mainValue"), value=main_value_prefix))
    if sub_value is not None and isinstance(sub_value, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("subValue"), value=sub_value))
    if sub_value and isinstance(sub_value, list):
        filters.append(dm.filters.In(view_id.as_property_ref("subValue"), values=sub_value))
    if sub_value_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("subValue"), value=sub_value_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None