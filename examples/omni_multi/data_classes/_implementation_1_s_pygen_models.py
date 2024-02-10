from __future__ import annotations

from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
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
    "Implementation1sPygenModels",
    "Implementation1sPygenModelsApply",
    "Implementation1sPygenModelsList",
    "Implementation1sPygenModelsApplyList",
    "Implementation1sPygenModelsFields",
    "Implementation1sPygenModelsTextFields",
]


Implementation1sPygenModelsTextFields = Literal["main_value", "sub_value", "value_1", "value_2"]
Implementation1sPygenModelsFields = Literal["main_value", "sub_value", "value_1", "value_2"]

_IMPLEMENTATION1SPYGENMODELS_PROPERTIES_BY_FIELD = {
    "main_value": "mainValue",
    "sub_value": "subValue",
    "value_1": "value1",
    "value_2": "value2",
}


class Implementation1sPygenModels(SubInterface):
    """This represents the reading version of implementation 1 s pygen model.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 s pygen model.
        data_record: The data record of the implementation 1 s pygen model node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
        value_2: The value 2 field.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    value_1: Optional[str] = Field(None, alias="value1")
    value_2: str = Field(alias="value2")

    def as_apply(self) -> Implementation1sPygenModelsApply:
        """Convert this read version of implementation 1 s pygen model to the writing version."""
        return Implementation1sPygenModelsApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_1=self.value_1,
            value_2=self.value_2,
        )


class Implementation1sPygenModelsApply(SubInterfaceApply):
    """This represents the writing version of implementation 1 s pygen model.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 s pygen model.
        data_record: The data record of the implementation 1 s pygen model node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
        value_2: The value 2 field.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
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
            Implementation1sPygenModels, dm.ViewId("pygen-models", "Implementation1", "1")
        )

        properties: dict[str, Any] = {}

        if self.main_value is not None or write_none:
            properties["mainValue"] = self.main_value

        if self.sub_value is not None or write_none:
            properties["subValue"] = self.sub_value

        if self.value_1 is not None or write_none:
            properties["value1"] = self.value_1

        if self.value_2 is not None:
            properties["value2"] = self.value_2

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
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


class Implementation1sPygenModelsList(DomainModelList[Implementation1sPygenModels]):
    """List of implementation 1 s pygen models in the read version."""

    _INSTANCE = Implementation1sPygenModels

    def as_apply(self) -> Implementation1sPygenModelsApplyList:
        """Convert these read versions of implementation 1 s pygen model to the writing versions."""
        return Implementation1sPygenModelsApplyList([node.as_write() for node in self.data])


class Implementation1sPygenModelsApplyList(DomainModelApplyList[Implementation1sPygenModelsApply]):
    """List of implementation 1 s pygen models in the writing version."""

    _INSTANCE = Implementation1sPygenModelsApply


def _create_implementation_1_s_pygen_model_filter(
    view_id: dm.ViewId,
    main_value: str | list[str] | None = None,
    main_value_prefix: str | None = None,
    sub_value: str | list[str] | None = None,
    sub_value_prefix: str | None = None,
    value_1: str | list[str] | None = None,
    value_1_prefix: str | None = None,
    value_2: str | list[str] | None = None,
    value_2_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(main_value, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("mainValue"), value=main_value))
    if main_value and isinstance(main_value, list):
        filters.append(dm.filters.In(view_id.as_property_ref("mainValue"), values=main_value))
    if main_value_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("mainValue"), value=main_value_prefix))
    if isinstance(sub_value, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("subValue"), value=sub_value))
    if sub_value and isinstance(sub_value, list):
        filters.append(dm.filters.In(view_id.as_property_ref("subValue"), values=sub_value))
    if sub_value_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("subValue"), value=sub_value_prefix))
    if isinstance(value_1, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("value1"), value=value_1))
    if value_1 and isinstance(value_1, list):
        filters.append(dm.filters.In(view_id.as_property_ref("value1"), values=value_1))
    if value_1_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("value1"), value=value_1_prefix))
    if isinstance(value_2, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("value2"), value=value_2))
    if value_2 and isinstance(value_2, list):
        filters.append(dm.filters.In(view_id.as_property_ref("value2"), values=value_2))
    if value_2_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("value2"), value=value_2_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
