from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

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
from ._main_interface import MainInterface, MainInterfaceWrite


__all__ = [
    "SubInterface",
    "SubInterfaceWrite",
    "SubInterfaceApply",
    "SubInterfaceList",
    "SubInterfaceWriteList",
    "SubInterfaceApplyList",
    "SubInterfaceFields",
    "SubInterfaceTextFields",
    "SubInterfaceGraphQL",
]


SubInterfaceTextFields = Literal["main_value", "sub_value"]
SubInterfaceFields = Literal["main_value", "sub_value"]

_SUBINTERFACE_PROPERTIES_BY_FIELD = {
    "main_value": "mainValue",
    "sub_value": "subValue",
}


class SubInterfaceGraphQL(GraphQLCore):
    """This represents the reading version of sub interface, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sub interface.
        data_record: The data record of the sub interface node.
        main_value: The main value field.
        sub_value: The sub value field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "SubInterface", "1")
    main_value: Optional[str] = Field(None, alias="mainValue")
    sub_value: Optional[str] = Field(None, alias="subValue")

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    def as_read(self) -> SubInterface:
        """Convert this GraphQL format of sub interface to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return SubInterface(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            main_value=self.main_value,
            sub_value=self.sub_value,
        )

    def as_write(self) -> SubInterfaceWrite:
        """Convert this GraphQL format of sub interface to the writing format."""
        return SubInterfaceWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            main_value=self.main_value,
            sub_value=self.sub_value,
        )


class SubInterface(MainInterface):
    """This represents the reading version of sub interface.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sub interface.
        data_record: The data record of the sub interface node.
        main_value: The main value field.
        sub_value: The sub value field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "SubInterface", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    sub_value: Optional[str] = Field(None, alias="subValue")

    def as_write(self) -> SubInterfaceWrite:
        """Convert this read version of sub interface to the writing version."""
        return SubInterfaceWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            main_value=self.main_value,
            sub_value=self.sub_value,
        )

    def as_apply(self) -> SubInterfaceWrite:
        """Convert this read version of sub interface to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SubInterfaceWrite(MainInterfaceWrite):
    """This represents the writing version of sub interface.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sub interface.
        data_record: The data record of the sub interface node.
        main_value: The main value field.
        sub_value: The sub value field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "SubInterface", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    sub_value: Optional[str] = Field(None, alias="subValue")

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


class SubInterfaceApply(SubInterfaceWrite):
    def __new__(cls, *args, **kwargs) -> SubInterfaceApply:
        warnings.warn(
            "SubInterfaceApply is deprecated and will be removed in v1.0. Use SubInterfaceWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "SubInterface.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class SubInterfaceList(DomainModelList[SubInterface]):
    """List of sub interfaces in the read version."""

    _INSTANCE = SubInterface

    def as_write(self) -> SubInterfaceWriteList:
        """Convert these read versions of sub interface to the writing versions."""
        return SubInterfaceWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> SubInterfaceWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SubInterfaceWriteList(DomainModelWriteList[SubInterfaceWrite]):
    """List of sub interfaces in the writing version."""

    _INSTANCE = SubInterfaceWrite


class SubInterfaceApplyList(SubInterfaceWriteList): ...


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
    filters: list[dm.Filter] = []
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
