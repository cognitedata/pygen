from __future__ import annotations

from typing import Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)
from ._sub_interface import SubInterface, SubInterfaceApply


__all__ = [
    "Implementation1v2",
    "Implementation1v2Apply",
    "Implementation1v2List",
    "Implementation1v2ApplyList",
    "Implementation1v2Fields",
    "Implementation1v2TextFields",
]


Implementation1v2TextFields = Literal["main_value", "sub_value", "value_2"]
Implementation1v2Fields = Literal["main_value", "sub_value", "value_2"]

_IMPLEMENTATION1V2_PROPERTIES_BY_FIELD = {
    "main_value": "mainValue",
    "sub_value": "subValue",
    "value_2": "value2",
}


class Implementation1v2(SubInterface):
    """This represents the reading version of implementation 1 v 2.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 v 2.
        main_value: The main value field.
        sub_value: The sub value field.
        value_2: The value 2 field.
        created_time: The created time of the implementation 1 v 2 node.
        last_updated_time: The last updated time of the implementation 1 v 2 node.
        deleted_time: If present, the deleted time of the implementation 1 v 2 node.
        version: The version of the implementation 1 v 2 node.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    value_2: str = Field(alias="value2")

    def as_apply(self) -> Implementation1v2Apply:
        """Convert this read version of implementation 1 v 2 to the writing version."""
        return Implementation1v2Apply(
            space=self.space,
            external_id=self.external_id,
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_2=self.value_2,
        )


class Implementation1v2Apply(SubInterfaceApply):
    """This represents the writing version of implementation 1 v 2.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 v 2.
        main_value: The main value field.
        sub_value: The sub value field.
        value_2: The value 2 field.
        existing_version: Fail the ingestion request if the implementation 1 v 2 version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    value_2: str = Field(alias="value2")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            Implementation1v2, dm.ViewId("pygen-models", "Implementation1", "2")
        )

        properties = {}

        if self.main_value is not None or write_none:
            properties["mainValue"] = self.main_value

        if self.sub_value is not None or write_none:
            properties["subValue"] = self.sub_value

        if self.value_2 is not None:
            properties["value2"] = self.value_2

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=self.node_type,
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


class Implementation1v2List(DomainModelList[Implementation1v2]):
    """List of implementation 1 v 2 in the read version."""

    _INSTANCE = Implementation1v2

    def as_apply(self) -> Implementation1v2ApplyList:
        """Convert these read versions of implementation 1 v 2 to the writing versions."""
        return Implementation1v2ApplyList([node.as_apply() for node in self.data])


class Implementation1v2ApplyList(DomainModelApplyList[Implementation1v2Apply]):
    """List of implementation 1 v 2 in the writing version."""

    _INSTANCE = Implementation1v2Apply


def _create_implementation_1_v_2_filter(
    view_id: dm.ViewId,
    main_value: str | list[str] | None = None,
    main_value_prefix: str | None = None,
    sub_value: str | list[str] | None = None,
    sub_value_prefix: str | None = None,
    value_2: str | list[str] | None = None,
    value_2_prefix: str | None = None,
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
    if value_2 is not None and isinstance(value_2, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("value2"), value=value_2))
    if value_2 and isinstance(value_2, list):
        filters.append(dm.filters.In(view_id.as_property_ref("value2"), values=value_2))
    if value_2_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("value2"), value=value_2_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
