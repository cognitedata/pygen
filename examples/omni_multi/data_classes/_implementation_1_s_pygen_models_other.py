from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

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


__all__ = [
    "Implementation1sPygenModelsOther",
    "Implementation1sPygenModelsOtherWrite",
    "Implementation1sPygenModelsOtherApply",
    "Implementation1sPygenModelsOtherList",
    "Implementation1sPygenModelsOtherWriteList",
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


class Implementation1sPygenModelsOtherGraphQL(GraphQLCore):
    """This represents the reading version of implementation 1 s pygen models other, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 s pygen models other.
        data_record: The data record of the implementation 1 s pygen models other node.
        main_value: The main value field.
        value_1: The value 1 field.
        value_2: The value 2 field.
    """

    view_id = dm.ViewId("pygen-models-other", "Implementation1", "1")
    main_value: Optional[str] = Field(None, alias="mainValue")
    value_1: Optional[str] = Field(None, alias="value1")
    value_2: Optional[str] = Field(None, alias="value2")

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

    def as_read(self) -> Implementation1sPygenModelsOther:
        """Convert this GraphQL format of implementation 1 s pygen models other to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Implementation1sPygenModelsOther(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            main_value=self.main_value,
            value_1=self.value_1,
            value_2=self.value_2,
        )

    def as_write(self) -> Implementation1sPygenModelsOtherWrite:
        """Convert this GraphQL format of implementation 1 s pygen models other to the writing format."""
        return Implementation1sPygenModelsOtherWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            main_value=self.main_value,
            value_1=self.value_1,
            value_2=self.value_2,
        )


class Implementation1sPygenModelsOther(DomainModel):
    """This represents the reading version of implementation 1 s pygen models other.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 s pygen models other.
        data_record: The data record of the implementation 1 s pygen models other node.
        main_value: The main value field.
        value_1: The value 1 field.
        value_2: The value 2 field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    main_value: Optional[str] = Field(None, alias="mainValue")
    value_1: Optional[str] = Field(None, alias="value1")
    value_2: str = Field(alias="value2")

    def as_write(self) -> Implementation1sPygenModelsOtherWrite:
        """Convert this read version of implementation 1 s pygen models other to the writing version."""
        return Implementation1sPygenModelsOtherWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            main_value=self.main_value,
            value_1=self.value_1,
            value_2=self.value_2,
        )

    def as_apply(self) -> Implementation1sPygenModelsOtherWrite:
        """Convert this read version of implementation 1 s pygen models other to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class Implementation1sPygenModelsOtherWrite(DomainModelWrite):
    """This represents the writing version of implementation 1 s pygen models other.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 s pygen models other.
        data_record: The data record of the implementation 1 s pygen models other node.
        main_value: The main value field.
        value_1: The value 1 field.
        value_2: The value 2 field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    main_value: Optional[str] = Field(None, alias="mainValue")
    value_1: Optional[str] = Field(None, alias="value1")
    value_2: str = Field(alias="value2")

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
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
                existing_version=None if allow_version_increase else self.data_record.existing_version,
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


class Implementation1sPygenModelsOtherApply(Implementation1sPygenModelsOtherWrite):
    def __new__(cls, *args, **kwargs) -> Implementation1sPygenModelsOtherApply:
        warnings.warn(
            "Implementation1sPygenModelsOtherApply is deprecated and will be removed in v1.0. Use Implementation1sPygenModelsOtherWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Implementation1sPygenModelsOther.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class Implementation1sPygenModelsOtherList(DomainModelList[Implementation1sPygenModelsOther]):
    """List of implementation 1 s pygen models others in the read version."""

    _INSTANCE = Implementation1sPygenModelsOther

    def as_write(self) -> Implementation1sPygenModelsOtherWriteList:
        """Convert these read versions of implementation 1 s pygen models other to the writing versions."""
        return Implementation1sPygenModelsOtherWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> Implementation1sPygenModelsOtherWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class Implementation1sPygenModelsOtherWriteList(DomainModelWriteList[Implementation1sPygenModelsOtherWrite]):
    """List of implementation 1 s pygen models others in the writing version."""

    _INSTANCE = Implementation1sPygenModelsOtherWrite


class Implementation1sPygenModelsOtherApplyList(Implementation1sPygenModelsOtherWriteList): ...


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
    if isinstance(main_value, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("mainValue"), value=main_value))
    if main_value and isinstance(main_value, list):
        filters.append(dm.filters.In(view_id.as_property_ref("mainValue"), values=main_value))
    if main_value_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("mainValue"), value=main_value_prefix))
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
