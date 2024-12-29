from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
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
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
)

if TYPE_CHECKING:
    from wind_turbine.data_classes._nacelle import Nacelle, NacelleList, NacelleGraphQL, NacelleWrite, NacelleWriteList
    from wind_turbine.data_classes._sensor_time_series import (
        SensorTimeSeries,
        SensorTimeSeriesList,
        SensorTimeSeriesGraphQL,
        SensorTimeSeriesWrite,
        SensorTimeSeriesWriteList,
    )


__all__ = [
    "Generator",
    "GeneratorWrite",
    "GeneratorApply",
    "GeneratorList",
    "GeneratorWriteList",
    "GeneratorApplyList",
    "GeneratorGraphQL",
]


GeneratorTextFields = Literal["external_id",]
GeneratorFields = Literal["external_id",]

_GENERATOR_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class GeneratorGraphQL(GraphQLCore):
    """This represents the reading version of generator, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        data_record: The data record of the generator node.
        generator_speed_controller: The generator speed controller field.
        generator_speed_controller_reference: The generator speed controller reference field.
        nacelle: The nacelle field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Generator", "1")
    generator_speed_controller: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    generator_speed_controller_reference: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    nacelle: Optional[NacelleGraphQL] = Field(default=None, repr=False)

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

    @field_validator("generator_speed_controller", "generator_speed_controller_reference", "nacelle", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Generator:
        """Convert this GraphQL format of generator to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Generator(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            generator_speed_controller=(
                self.generator_speed_controller.as_read()
                if isinstance(self.generator_speed_controller, GraphQLCore)
                else self.generator_speed_controller
            ),
            generator_speed_controller_reference=(
                self.generator_speed_controller_reference.as_read()
                if isinstance(self.generator_speed_controller_reference, GraphQLCore)
                else self.generator_speed_controller_reference
            ),
            nacelle=self.nacelle.as_read() if isinstance(self.nacelle, GraphQLCore) else self.nacelle,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> GeneratorWrite:
        """Convert this GraphQL format of generator to the writing format."""
        return GeneratorWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            generator_speed_controller=(
                self.generator_speed_controller.as_write()
                if isinstance(self.generator_speed_controller, GraphQLCore)
                else self.generator_speed_controller
            ),
            generator_speed_controller_reference=(
                self.generator_speed_controller_reference.as_write()
                if isinstance(self.generator_speed_controller_reference, GraphQLCore)
                else self.generator_speed_controller_reference
            ),
        )


class Generator(DomainModel):
    """This represents the reading version of generator.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        data_record: The data record of the generator node.
        generator_speed_controller: The generator speed controller field.
        generator_speed_controller_reference: The generator speed controller reference field.
        nacelle: The nacelle field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Generator", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    generator_speed_controller: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    generator_speed_controller_reference: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(
        default=None, repr=False
    )
    nacelle: Optional[Nacelle] = Field(default=None, repr=False)

    @field_validator("generator_speed_controller", "generator_speed_controller_reference", "nacelle", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> GeneratorWrite:
        """Convert this read version of generator to the writing version."""
        return GeneratorWrite.model_validate(as_write_args(self))

    def as_apply(self) -> GeneratorWrite:
        """Convert this read version of generator to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class GeneratorWrite(DomainModelWrite):
    """This represents the writing version of generator.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        data_record: The data record of the generator node.
        generator_speed_controller: The generator speed controller field.
        generator_speed_controller_reference: The generator speed controller reference field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "generator_speed_controller",
        "generator_speed_controller_reference",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "generator_speed_controller",
        "generator_speed_controller_reference",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Generator", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    generator_speed_controller: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    generator_speed_controller_reference: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False
    )

    @field_validator("generator_speed_controller", "generator_speed_controller_reference", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class GeneratorApply(GeneratorWrite):
    def __new__(cls, *args, **kwargs) -> GeneratorApply:
        warnings.warn(
            "GeneratorApply is deprecated and will be removed in v1.0. "
            "Use GeneratorWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Generator.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class GeneratorList(DomainModelList[Generator]):
    """List of generators in the read version."""

    _INSTANCE = Generator

    def as_write(self) -> GeneratorWriteList:
        """Convert these read versions of generator to the writing versions."""
        return GeneratorWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> GeneratorWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def generator_speed_controller(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.generator_speed_controller
                for item in self.data
                if isinstance(item.generator_speed_controller, SensorTimeSeries)
            ]
        )

    @property
    def generator_speed_controller_reference(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.generator_speed_controller_reference
                for item in self.data
                if isinstance(item.generator_speed_controller_reference, SensorTimeSeries)
            ]
        )

    @property
    def nacelle(self) -> NacelleList:
        from ._nacelle import Nacelle, NacelleList

        return NacelleList([item.nacelle for item in self.data if isinstance(item.nacelle, Nacelle)])


class GeneratorWriteList(DomainModelWriteList[GeneratorWrite]):
    """List of generators in the writing version."""

    _INSTANCE = GeneratorWrite

    @property
    def generator_speed_controller(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.generator_speed_controller
                for item in self.data
                if isinstance(item.generator_speed_controller, SensorTimeSeriesWrite)
            ]
        )

    @property
    def generator_speed_controller_reference(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.generator_speed_controller_reference
                for item in self.data
                if isinstance(item.generator_speed_controller_reference, SensorTimeSeriesWrite)
            ]
        )


class GeneratorApplyList(GeneratorWriteList): ...


def _create_generator_filter(
    view_id: dm.ViewId,
    generator_speed_controller: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    generator_speed_controller_reference: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(generator_speed_controller, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        generator_speed_controller
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("generator_speed_controller"),
                value=as_instance_dict_id(generator_speed_controller),
            )
        )
    if (
        generator_speed_controller
        and isinstance(generator_speed_controller, Sequence)
        and not isinstance(generator_speed_controller, str)
        and not is_tuple_id(generator_speed_controller)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("generator_speed_controller"),
                values=[as_instance_dict_id(item) for item in generator_speed_controller],
            )
        )
    if isinstance(generator_speed_controller_reference, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        generator_speed_controller_reference
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("generator_speed_controller_reference"),
                value=as_instance_dict_id(generator_speed_controller_reference),
            )
        )
    if (
        generator_speed_controller_reference
        and isinstance(generator_speed_controller_reference, Sequence)
        and not isinstance(generator_speed_controller_reference, str)
        and not is_tuple_id(generator_speed_controller_reference)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("generator_speed_controller_reference"),
                values=[as_instance_dict_id(item) for item in generator_speed_controller_reference],
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _GeneratorQuery(NodeQueryCore[T_DomainModelList, GeneratorList]):
    _view_id = Generator._view_id
    _result_cls = Generator
    _result_list_cls_end = GeneratorList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._nacelle import _NacelleQuery
        from ._sensor_time_series import _SensorTimeSeriesQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _SensorTimeSeriesQuery not in created_types:
            self.generator_speed_controller = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("generator_speed_controller"),
                    direction="outwards",
                ),
                connection_name="generator_speed_controller",
                connection_property=ViewPropertyId(self._view_id, "generator_speed_controller"),
            )

        if _SensorTimeSeriesQuery not in created_types:
            self.generator_speed_controller_reference = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("generator_speed_controller_reference"),
                    direction="outwards",
                ),
                connection_name="generator_speed_controller_reference",
                connection_property=ViewPropertyId(self._view_id, "generator_speed_controller_reference"),
            )

        if _NacelleQuery not in created_types:
            self.nacelle = _NacelleQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_power", "Nacelle", "1").as_property_ref("main_shaft"),
                    direction="inwards",
                ),
                connection_name="nacelle",
                connection_property=ViewPropertyId(self._view_id, "nacelle"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])

    def list_generator(self, limit: int = DEFAULT_QUERY_LIMIT) -> GeneratorList:
        return self._list(limit=limit)


class GeneratorQuery(_GeneratorQuery[GeneratorList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, GeneratorList)
