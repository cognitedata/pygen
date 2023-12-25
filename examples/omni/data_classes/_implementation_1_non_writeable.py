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
    "Implementation1NonWriteable",
    "Implementation1NonWriteableApply",
    "Implementation1NonWriteableList",
    "Implementation1NonWriteableApplyList",
    "Implementation1NonWriteableFields",
    "Implementation1NonWriteableTextFields",
]


Implementation1NonWriteableTextFields = Literal["main_value", "sub_value", "value_1"]
Implementation1NonWriteableFields = Literal["main_value", "sub_value", "value_1"]

_IMPLEMENTATION1NONWRITEABLE_PROPERTIES_BY_FIELD = {
    "main_value": "mainValue",
    "sub_value": "subValue",
    "value_1": "value1",
}


class Implementation1NonWriteable(DomainModel):
    """This represents the reading version of implementation 1 non writeable.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 non writeable.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
        created_time: The created time of the implementation 1 non writeable node.
        last_updated_time: The last updated time of the implementation 1 non writeable node.
        deleted_time: If present, the deleted time of the implementation 1 non writeable node.
        version: The version of the implementation 1 non writeable node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    main_value: Optional[str] = Field(None, alias="mainValue")
    sub_value: Optional[str] = Field(None, alias="subValue")
    value_1: Optional[str] = Field(None, alias="value1")

    def as_apply(self) -> Implementation1NonWriteableApply:
        """Convert this read version of implementation 1 non writeable to the writing version."""
        return Implementation1NonWriteableApply(
            space=self.space,
            external_id=self.external_id,
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_1=self.value_1,
        )


class Implementation1NonWriteableApply(DomainModelApply):
    """This represents the writing version of implementation 1 non writeable.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 non writeable.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
        existing_version: Fail the ingestion request if the implementation 1 non writeable version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    main_value: Optional[str] = Field(None, alias="mainValue")
    sub_value: Optional[str] = Field(None, alias="subValue")
    value_1: Optional[str] = Field(None, alias="value1")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "pygen-models", "Implementation1NonWriteable", "1"
        )

        properties = {}

        if self.main_value is not None:
            properties["mainValue"] = self.main_value

        if self.sub_value is not None:
            properties["subValue"] = self.sub_value

        if self.value_1 is not None:
            properties["value1"] = self.value_1

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("pygen-models", "Implementation1"),
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


class Implementation1NonWriteableList(DomainModelList[Implementation1NonWriteable]):
    """List of implementation 1 non writeables in the read version."""

    _INSTANCE = Implementation1NonWriteable

    def as_apply(self) -> Implementation1NonWriteableApplyList:
        """Convert these read versions of implementation 1 non writeable to the writing versions."""
        return Implementation1NonWriteableApplyList([node.as_apply() for node in self.data])


class Implementation1NonWriteableApplyList(DomainModelApplyList[Implementation1NonWriteableApply]):
    """List of implementation 1 non writeables in the writing version."""

    _INSTANCE = Implementation1NonWriteableApply


def _create_implementation_1_non_writeable_filter(
    view_id: dm.ViewId,
    main_value: str | list[str] | None = None,
    main_value_prefix: str | None = None,
    sub_value: str | list[str] | None = None,
    sub_value_prefix: str | None = None,
    value_1: str | list[str] | None = None,
    value_1_prefix: str | None = None,
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
    if value_1 is not None and isinstance(value_1, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("value1"), value=value_1))
    if value_1 and isinstance(value_1, list):
        filters.append(dm.filters.In(view_id.as_property_ref("value1"), values=value_1))
    if value_1_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("value1"), value=value_1_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
