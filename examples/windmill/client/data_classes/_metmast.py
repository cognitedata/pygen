from __future__ import annotations

from typing import Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)


__all__ = ["Metmast", "MetmastApply", "MetmastList", "MetmastApplyList", "MetmastFields", "MetmastTextFields"]


MetmastTextFields = Literal["temperature", "tilt_angle", "wind_speed"]
MetmastFields = Literal["position", "temperature", "tilt_angle", "wind_speed"]

_METMAST_PROPERTIES_BY_FIELD = {
    "position": "position",
    "temperature": "temperature",
    "tilt_angle": "tilt_angle",
    "wind_speed": "wind_speed",
}


class Metmast(DomainModel):
    """This represents the reading version of metmast.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the metmast.
        position: The position field.
        temperature: The temperature field.
        tilt_angle: The tilt angle field.
        wind_speed: The wind speed field.
        created_time: The created time of the metmast node.
        last_updated_time: The last updated time of the metmast node.
        deleted_time: If present, the deleted time of the metmast node.
        version: The version of the metmast node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    position: Optional[float] = None
    temperature: Union[TimeSeries, str, None] = None
    tilt_angle: Union[TimeSeries, str, None] = None
    wind_speed: Union[TimeSeries, str, None] = None

    def as_apply(self) -> MetmastApply:
        """Convert this read version of metmast to the writing version."""
        return MetmastApply(
            space=self.space,
            external_id=self.external_id,
            position=self.position,
            temperature=self.temperature,
            tilt_angle=self.tilt_angle,
            wind_speed=self.wind_speed,
        )


class MetmastApply(DomainModelApply):
    """This represents the writing version of metmast.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the metmast.
        position: The position field.
        temperature: The temperature field.
        tilt_angle: The tilt angle field.
        wind_speed: The wind speed field.
        existing_version: Fail the ingestion request if the metmast version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    position: Optional[float] = None
    temperature: Union[TimeSeries, str, None] = None
    tilt_angle: Union[TimeSeries, str, None] = None
    wind_speed: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-models", "Metmast", "1"
        )

        properties = {}

        if self.position is not None:
            properties["position"] = self.position

        if self.temperature is not None:
            properties["temperature"] = (
                self.temperature if isinstance(self.temperature, str) else self.temperature.external_id
            )

        if self.tilt_angle is not None:
            properties["tilt_angle"] = (
                self.tilt_angle if isinstance(self.tilt_angle, str) else self.tilt_angle.external_id
            )

        if self.wind_speed is not None:
            properties["wind_speed"] = (
                self.wind_speed if isinstance(self.wind_speed, str) else self.wind_speed.external_id
            )

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.temperature, CogniteTimeSeries):
            resources.time_series.append(self.temperature)

        if isinstance(self.tilt_angle, CogniteTimeSeries):
            resources.time_series.append(self.tilt_angle)

        if isinstance(self.wind_speed, CogniteTimeSeries):
            resources.time_series.append(self.wind_speed)

        return resources


class MetmastList(DomainModelList[Metmast]):
    """List of metmasts in the read version."""

    _INSTANCE = Metmast

    def as_apply(self) -> MetmastApplyList:
        """Convert these read versions of metmast to the writing versions."""
        return MetmastApplyList([node.as_apply() for node in self.data])


class MetmastApplyList(DomainModelApplyList[MetmastApply]):
    """List of metmasts in the writing version."""

    _INSTANCE = MetmastApply


def _create_metmast_filter(
    view_id: dm.ViewId,
    min_position: float | None = None,
    max_position: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_position or max_position:
        filters.append(dm.filters.Range(view_id.as_property_ref("position"), gte=min_position, lte=max_position))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None