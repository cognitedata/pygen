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
    "MainInterface",
    "MainInterfaceApply",
    "MainInterfaceList",
    "MainInterfaceApplyList",
    "MainInterfaceFields",
    "MainInterfaceTextFields",
]


MainInterfaceTextFields = Literal["main_value"]
MainInterfaceFields = Literal["main_value"]

_MAININTERFACE_PROPERTIES_BY_FIELD = {
    "main_value": "mainValue",
}


class MainInterface(DomainModel):
    """This represents the reading version of main interface.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the main interface.
        main_value: The main value field.
        created_time: The created time of the main interface node.
        last_updated_time: The last updated time of the main interface node.
        deleted_time: If present, the deleted time of the main interface node.
        version: The version of the main interface node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    main_value: Optional[str] = Field(None, alias="mainValue")

    def as_apply(self) -> MainInterfaceApply:
        """Convert this read version of main interface to the writing version."""
        return MainInterfaceApply(
            space=self.space,
            external_id=self.external_id,
            main_value=self.main_value,
        )


class MainInterfaceApply(DomainModelApply):
    """This represents the writing version of main interface.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the main interface.
        main_value: The main value field.
        existing_version: Fail the ingestion request if the main interface version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    main_value: Optional[str] = Field(None, alias="mainValue")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "pygen-models", "MainInterface", "1"
        )

        properties = {}

        if self.main_value is not None:
            properties["mainValue"] = self.main_value

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


class MainInterfaceList(DomainModelList[MainInterface]):
    """List of main interfaces in the read version."""

    _INSTANCE = MainInterface

    def as_apply(self) -> MainInterfaceApplyList:
        """Convert these read versions of main interface to the writing versions."""
        return MainInterfaceApplyList([node.as_apply() for node in self.data])


class MainInterfaceApplyList(DomainModelApplyList[MainInterfaceApply]):
    """List of main interfaces in the writing version."""

    _INSTANCE = MainInterfaceApply


def _create_main_interface_filter(
    view_id: dm.ViewId,
    main_value: str | list[str] | None = None,
    main_value_prefix: str | None = None,
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
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None