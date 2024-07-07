from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import validator, root_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)
from ._sub_interface import SubInterface, SubInterfaceWrite


__all__ = [
    "Implementation1v2",
    "Implementation1v2Write",
    "Implementation1v2Apply",
    "Implementation1v2List",
    "Implementation1v2WriteList",
    "Implementation1v2ApplyList",
    "Implementation1v2Fields",
    "Implementation1v2TextFields",
    "Implementation1v2GraphQL",
]


Implementation1v2TextFields = Literal["main_value", "sub_value", "value_2"]
Implementation1v2Fields = Literal["main_value", "sub_value", "value_2"]

_IMPLEMENTATION1V2_PROPERTIES_BY_FIELD = {
    "main_value": "mainValue",
    "sub_value": "subValue",
    "value_2": "value2",
}


class Implementation1v2GraphQL(GraphQLCore):
    """This represents the reading version of implementation 1 v 2, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 v 2.
        data_record: The data record of the implementation 1 v 2 node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_2: The value 2 field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "Implementation1", "2")
    main_value: Optional[str] = Field(None, alias="mainValue")
    sub_value: Optional[str] = Field(None, alias="subValue")
    value_2: Optional[str] = Field(None, alias="value2")

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

    def as_read(self) -> Implementation1v2:
        """Convert this GraphQL format of implementation 1 v 2 to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Implementation1v2(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_2=self.value_2,
        )

    def as_write(self) -> Implementation1v2Write:
        """Convert this GraphQL format of implementation 1 v 2 to the writing format."""
        return Implementation1v2Write(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_2=self.value_2,
        )


class Implementation1v2(SubInterface):
    """This represents the reading version of implementation 1 v 2.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 v 2.
        data_record: The data record of the implementation 1 v 2 node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_2: The value 2 field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "Implementation1", "2")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    value_2: str = Field(alias="value2")

    def as_write(self) -> Implementation1v2Write:
        """Convert this read version of implementation 1 v 2 to the writing version."""
        return Implementation1v2Write(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_2=self.value_2,
        )

    def as_apply(self) -> Implementation1v2Write:
        """Convert this read version of implementation 1 v 2 to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class Implementation1v2Write(SubInterfaceWrite):
    """This represents the writing version of implementation 1 v 2.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 v 2.
        data_record: The data record of the implementation 1 v 2 node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_2: The value 2 field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "Implementation1", "2")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    value_2: str = Field(alias="value2")

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

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
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class Implementation1v2Apply(Implementation1v2Write):
    def __new__(cls, *args, **kwargs) -> Implementation1v2Apply:
        warnings.warn(
            "Implementation1v2Apply is deprecated and will be removed in v1.0. Use Implementation1v2Write instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Implementation1v2.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class Implementation1v2List(DomainModelList[Implementation1v2]):
    """List of implementation 1 v 2 in the read version."""

    _INSTANCE = Implementation1v2

    def as_write(self) -> Implementation1v2WriteList:
        """Convert these read versions of implementation 1 v 2 to the writing versions."""
        return Implementation1v2WriteList([node.as_write() for node in self.data])

    def as_apply(self) -> Implementation1v2WriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class Implementation1v2WriteList(DomainModelWriteList[Implementation1v2Write]):
    """List of implementation 1 v 2 in the writing version."""

    _INSTANCE = Implementation1v2Write


class Implementation1v2ApplyList(Implementation1v2WriteList): ...


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
