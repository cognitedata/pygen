from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import validator, root_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)
from ._sub_interface import SubInterface


__all__ = [
    "Implementation1NonWriteable",
    "Implementation1NonWriteableList",
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


class Implementation1NonWriteableGraphQL(GraphQLCore):
    """This represents the reading version of implementation 1 non writeable, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 non writeable.
        data_record: The data record of the implementation 1 non writeable node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
    """

    view_id = dm.ViewId("pygen-models", "Implementation1NonWriteable", "1")
    main_value: Optional[str] = Field(None, alias="mainValue")
    sub_value: Optional[str] = Field(None, alias="subValue")
    value_1: Optional[str] = Field(None, alias="value1")

    @root_validator(pre=True)
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    def as_read(self) -> Implementation1NonWriteable:
        """Convert this GraphQL format of implementation 1 non writeable to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Implementation1NonWriteable(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_1=self.value_1,
        )


class Implementation1NonWriteable(SubInterface):
    """This represents the reading version of implementation 1 non writeable.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 non writeable.
        data_record: The data record of the implementation 1 non writeable node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    value_1: Optional[str] = Field(None, alias="value1")


class Implementation1NonWriteableList(DomainModelList[Implementation1NonWriteable]):
    """List of implementation 1 non writeables in the read version."""

    _INSTANCE = Implementation1NonWriteable


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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
