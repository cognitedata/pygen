from __future__ import annotations

from typing import Any, Literal, Optional, Union

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


__all__ = [
    "Implementation1sPygenModelsOther",
    "Implementation1sPygenModelsOtherApply",
    "Implementation1sPygenModelsOtherList",
    "Implementation1sPygenModelsOtherApplyList",
    "Implementation1sPygenModelsOtherFields",
    "Implementation1sPygenModelsOtherTextFields",
]


Implementation1sPygenModelsOtherTextFields = Literal["main_value", "value_1", "value_2"]
Implementation1sPygenModelsOtherFields = Literal["main_value", "value_1", "value_2"]

_IMPLEMENTATION1SPYGENMODELSOTHER_PROPERTIES_BY_FIELD = {
    "main_value": "mainValue",
    "value_1": "value1",
    "value_2": "value2",
}


class Implementation1sPygenModelsOther(DomainModel):
    """This represents the reading version of implementation 1 s pygen models other.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 s pygen models other.
        main_value: The main value field.
        value_1: The value 1 field.
        value_2: The value 2 field.
        created_time: The created time of the implementation 1 s pygen models other node.
        last_updated_time: The last updated time of the implementation 1 s pygen models other node.
        deleted_time: If present, the deleted time of the implementation 1 s pygen models other node.
        version: The version of the implementation 1 s pygen models other node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    main_value: Optional[str] = Field(None, alias="mainValue")
    value_1: Optional[str] = Field(None, alias="value1")
    value_2: str = Field(alias="value2")

    def as_apply(self) -> Implementation1sPygenModelsOtherApply:
        """Convert this read version of implementation 1 s pygen models other to the writing version."""
        return Implementation1sPygenModelsOtherApply(
            space=self.space,
            external_id=self.external_id,
            main_value=self.main_value,
            value_1=self.value_1,
            value_2=self.value_2,
        )


class Implementation1sPygenModelsOtherApply(DomainModelApply):
    """This represents the writing version of implementation 1 s pygen models other.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 s pygen models other.
        main_value: The main value field.
        value_1: The value 1 field.
        value_2: The value 2 field.
        existing_version: Fail the ingestion request if the implementation 1 s pygen models other version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    main_value: Optional[str] = Field(None, alias="mainValue")
    value_1: Optional[str] = Field(None, alias="value1")
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
            Implementation1sPygenModelsOther, dm.ViewId("pygen-models-other", "Implementation1", "1")
        )

        properties: dict[str, Any] = {}

        if self.main_value is not None or write_none:
            properties["mainValue"] = self.main_value

        if self.value_1 is not None or write_none:
            properties["value1"] = self.value_1

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


class Implementation1sPygenModelsOtherList(DomainModelList[Implementation1sPygenModelsOther]):
    """List of implementation 1 s pygen models others in the read version."""

    _INSTANCE = Implementation1sPygenModelsOther

    def as_apply(self) -> Implementation1sPygenModelsOtherApplyList:
        """Convert these read versions of implementation 1 s pygen models other to the writing versions."""
        return Implementation1sPygenModelsOtherApplyList([node.as_apply() for node in self.data])


class Implementation1sPygenModelsOtherApplyList(DomainModelApplyList[Implementation1sPygenModelsOtherApply]):
    """List of implementation 1 s pygen models others in the writing version."""

    _INSTANCE = Implementation1sPygenModelsOtherApply


def _create_implementation_1_s_pygen_models_other_filter(
    view_id: dm.ViewId,
    main_value: str | list[str] | None = None,
    main_value_prefix: str | None = None,
    value_1: str | list[str] | None = None,
    value_1_prefix: str | None = None,
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
    if value_1 is not None and isinstance(value_1, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("value1"), value=value_1))
    if value_1 and isinstance(value_1, list):
        filters.append(dm.filters.In(view_id.as_property_ref("value1"), values=value_1))
    if value_1_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("value1"), value=value_1_prefix))
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