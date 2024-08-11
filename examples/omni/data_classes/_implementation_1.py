from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
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
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    T_DomainModelList,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    select_best_node,
    QueryCore,
)
from ._sub_interface import SubInterface, SubInterfaceWrite


__all__ = [
    "Implementation1",
    "Implementation1Write",
    "Implementation1Apply",
    "Implementation1List",
    "Implementation1WriteList",
    "Implementation1ApplyList",
    "Implementation1Fields",
    "Implementation1TextFields",
    "Implementation1GraphQL",
]


Implementation1TextFields = Literal["main_value", "sub_value", "value_1", "value_2"]
Implementation1Fields = Literal["main_value", "sub_value", "value_1", "value_2"]

_IMPLEMENTATION1_PROPERTIES_BY_FIELD = {
    "main_value": "mainValue",
    "sub_value": "subValue",
    "value_1": "value1",
    "value_2": "value2",
}


class Implementation1GraphQL(GraphQLCore):
    """This represents the reading version of implementation 1, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1.
        data_record: The data record of the implementation 1 node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
        value_2: The value 2 field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "Implementation1", "1")
    main_value: Optional[str] = Field(None, alias="mainValue")
    sub_value: Optional[str] = Field(None, alias="subValue")
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Implementation1:
        """Convert this GraphQL format of implementation 1 to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Implementation1(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_1=self.value_1,
            value_2=self.value_2,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> Implementation1Write:
        """Convert this GraphQL format of implementation 1 to the writing format."""
        return Implementation1Write(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_1=self.value_1,
            value_2=self.value_2,
        )


class Implementation1(SubInterface):
    """This represents the reading version of implementation 1.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1.
        data_record: The data record of the implementation 1 node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
        value_2: The value 2 field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "Implementation1", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    value_1: Optional[str] = Field(None, alias="value1")
    value_2: str = Field(alias="value2")

    def as_write(self) -> Implementation1Write:
        """Convert this read version of implementation 1 to the writing version."""
        return Implementation1Write(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_1=self.value_1,
            value_2=self.value_2,
        )

    def as_apply(self) -> Implementation1Write:
        """Convert this read version of implementation 1 to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class Implementation1Write(SubInterfaceWrite):
    """This represents the writing version of implementation 1.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1.
        data_record: The data record of the implementation 1 node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
        value_2: The value 2 field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "Implementation1", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    value_1: Optional[str] = Field(None, alias="value1")
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
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class Implementation1Apply(Implementation1Write):
    def __new__(cls, *args, **kwargs) -> Implementation1Apply:
        warnings.warn(
            "Implementation1Apply is deprecated and will be removed in v1.0. Use Implementation1Write instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Implementation1.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class Implementation1List(DomainModelList[Implementation1]):
    """List of implementation 1 in the read version."""

    _INSTANCE = Implementation1

    def as_write(self) -> Implementation1WriteList:
        """Convert these read versions of implementation 1 to the writing versions."""
        return Implementation1WriteList([node.as_write() for node in self.data])

    def as_apply(self) -> Implementation1WriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class Implementation1WriteList(DomainModelWriteList[Implementation1Write]):
    """List of implementation 1 in the writing version."""

    _INSTANCE = Implementation1Write


class Implementation1ApplyList(Implementation1WriteList): ...


def _create_implementation_1_filter(
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


class _Implementation1Query(QueryCore[T_DomainModelList, Implementation1List]):
    _view_id = Implementation1._view_id
    _result_cls = Implementation1
    _result_list_cls_end = Implementation1List

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
    ):

        super().__init__(created_types, creation_path, client, result_list_cls, expression)

    def _assemble_filter(self) -> dm.filters.Filter:
        return dm.filters.HasData(views=[self._view_id])


class Implementation1Query(_Implementation1Query[Implementation1List]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, Implementation1List)
