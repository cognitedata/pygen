from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
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
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
    TimeSeries,
    TimeSeriesWrite,
    TimeSeriesGraphQL,
    T_DomainModelList,
    as_direct_relation_reference,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    select_best_node,
    QueryCore,
    NodeQueryCore,
)


__all__ = [
    "Rotor",
    "RotorWrite",
    "RotorApply",
    "RotorList",
    "RotorWriteList",
    "RotorApplyList",
    "RotorFields",
    "RotorTextFields",
    "RotorGraphQL",
]


RotorTextFields = Literal["external_id", "rotor_speed_controller", "rpm_low_speed_shaft"]
RotorFields = Literal["external_id", "rotor_speed_controller", "rpm_low_speed_shaft"]

_ROTOR_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "rotor_speed_controller": "rotor_speed_controller",
    "rpm_low_speed_shaft": "rpm_low_speed_shaft",
}

class RotorGraphQL(GraphQLCore):
    """This represents the reading version of rotor, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rotor.
        data_record: The data record of the rotor node.
        rotor_speed_controller: The rotor speed controller field.
        rpm_low_speed_shaft: The rpm low speed shaft field.
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Rotor", "1")
    rotor_speed_controller: Optional[TimeSeriesGraphQL] = None
    rpm_low_speed_shaft: Optional[TimeSeriesGraphQL] = None

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
    def as_read(self) -> Rotor:
        """Convert this GraphQL format of rotor to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Rotor(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            rotor_speed_controller=self.rotor_speed_controller.as_read() if self.rotor_speed_controller else None,
            rpm_low_speed_shaft=self.rpm_low_speed_shaft.as_read() if self.rpm_low_speed_shaft else None,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> RotorWrite:
        """Convert this GraphQL format of rotor to the writing format."""
        return RotorWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            rotor_speed_controller=self.rotor_speed_controller.as_write() if self.rotor_speed_controller else None,
            rpm_low_speed_shaft=self.rpm_low_speed_shaft.as_write() if self.rpm_low_speed_shaft else None,
        )


class Rotor(DomainModel):
    """This represents the reading version of rotor.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rotor.
        data_record: The data record of the rotor node.
        rotor_speed_controller: The rotor speed controller field.
        rpm_low_speed_shaft: The rpm low speed shaft field.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Rotor", "1")
    
    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    rotor_speed_controller: Union[TimeSeries, str, None] = None
    rpm_low_speed_shaft: Union[TimeSeries, str, None] = None

    def as_write(self) -> RotorWrite:
        """Convert this read version of rotor to the writing version."""
        return RotorWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            rotor_speed_controller=self.rotor_speed_controller.as_write() if isinstance(self.rotor_speed_controller, CogniteTimeSeries) else self.rotor_speed_controller,
            rpm_low_speed_shaft=self.rpm_low_speed_shaft.as_write() if isinstance(self.rpm_low_speed_shaft, CogniteTimeSeries) else self.rpm_low_speed_shaft,
        )

    def as_apply(self) -> RotorWrite:
        """Convert this read version of rotor to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class RotorWrite(DomainModelWrite):
    """This represents the writing version of rotor.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rotor.
        data_record: The data record of the rotor node.
        rotor_speed_controller: The rotor speed controller field.
        rpm_low_speed_shaft: The rpm low speed shaft field.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Rotor", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    rotor_speed_controller: Union[TimeSeriesWrite, str, None] = None
    rpm_low_speed_shaft: Union[TimeSeriesWrite, str, None] = None

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
        
        if self.rotor_speed_controller is not None or write_none:
            properties["rotor_speed_controller"] = self.rotor_speed_controller if isinstance(self.rotor_speed_controller, str) or self.rotor_speed_controller is None else self.rotor_speed_controller.external_id
        
        if self.rpm_low_speed_shaft is not None or write_none:
            properties["rpm_low_speed_shaft"] = self.rpm_low_speed_shaft if isinstance(self.rpm_low_speed_shaft, str) or self.rpm_low_speed_shaft is None else self.rpm_low_speed_shaft.external_id
        

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())
        


        if isinstance(self.rotor_speed_controller, CogniteTimeSeriesWrite):
            resources.time_series.append(self.rotor_speed_controller)

        if isinstance(self.rpm_low_speed_shaft, CogniteTimeSeriesWrite):
            resources.time_series.append(self.rpm_low_speed_shaft)

        return resources


class RotorApply(RotorWrite):
    def __new__(cls, *args, **kwargs) -> RotorApply:
        warnings.warn(
            "RotorApply is deprecated and will be removed in v1.0. Use RotorWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Rotor.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class RotorList(DomainModelList[Rotor]):
    """List of rotors in the read version."""

    _INSTANCE = Rotor

    def as_write(self) -> RotorWriteList:
        """Convert these read versions of rotor to the writing versions."""
        return RotorWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> RotorWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class RotorWriteList(DomainModelWriteList[RotorWrite]):
    """List of rotors in the writing version."""

    _INSTANCE = RotorWrite

class RotorApplyList(RotorWriteList): ...



def _create_rotor_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _RotorQuery(NodeQueryCore[T_DomainModelList, RotorList]):
    _view_id = Rotor._view_id
    _result_cls = Rotor
    _result_list_cls_end = RotorList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
    ):

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
        )



class RotorQuery(_RotorQuery[RotorList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, RotorList)
