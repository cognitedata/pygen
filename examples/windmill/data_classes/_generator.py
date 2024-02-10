from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
    TimeSeries,
)


__all__ = [
    "Generator",
    "GeneratorWrite",
    "GeneratorApply",
    "GeneratorList",
    "GeneratorWriteList",
    "GeneratorApplyList",
    "GeneratorFields",
    "GeneratorTextFields",
]


GeneratorTextFields = Literal["generator_speed_controller", "generator_speed_controller_reference"]
GeneratorFields = Literal["generator_speed_controller", "generator_speed_controller_reference"]

_GENERATOR_PROPERTIES_BY_FIELD = {
    "generator_speed_controller": "generator_speed_controller",
    "generator_speed_controller_reference": "generator_speed_controller_reference",
}


class Generator(DomainModel):
    """This represents the reading version of generator.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        data_record: The data record of the generator node.
        generator_speed_controller: The generator speed controller field.
        generator_speed_controller_reference: The generator speed controller reference field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    generator_speed_controller: Union[TimeSeries, str, None] = None
    generator_speed_controller_reference: Union[TimeSeries, str, None] = None

    def as_write(self) -> GeneratorWrite:
        """Convert this read version of generator to the writing version."""
        return GeneratorWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            generator_speed_controller=self.generator_speed_controller,
            generator_speed_controller_reference=self.generator_speed_controller_reference,
        )

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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    generator_speed_controller: Union[TimeSeries, str, None] = None
    generator_speed_controller_reference: Union[TimeSeries, str, None] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Generator, dm.ViewId("power-models", "Generator", "1"))

        properties: dict[str, Any] = {}

        if self.generator_speed_controller is not None or write_none:
            if isinstance(self.generator_speed_controller, str) or self.generator_speed_controller is None:
                properties["generator_speed_controller"] = self.generator_speed_controller
            else:
                properties["generator_speed_controller"] = self.generator_speed_controller.external_id

        if self.generator_speed_controller_reference is not None or write_none:
            if (
                isinstance(self.generator_speed_controller_reference, str)
                or self.generator_speed_controller_reference is None
            ):
                properties["generator_speed_controller_reference"] = self.generator_speed_controller_reference
            else:
                properties["generator_speed_controller_reference"] = (
                    self.generator_speed_controller_reference.external_id
                )

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

        if isinstance(self.generator_speed_controller, CogniteTimeSeries):
            resources.time_series.append(self.generator_speed_controller)

        if isinstance(self.generator_speed_controller_reference, CogniteTimeSeries):
            resources.time_series.append(self.generator_speed_controller_reference)

        return resources


class GeneratorApply(GeneratorWrite):
    def __new__(cls, *args, **kwargs) -> GeneratorApply:
        warnings.warn(
            "GeneratorApply is deprecated and will be removed in v1.0. Use GeneratorWrite instead."
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


class GeneratorWriteList(DomainModelWriteList[GeneratorWrite]):
    """List of generators in the writing version."""

    _INSTANCE = GeneratorWrite


class GeneratorApplyList(GeneratorWriteList): ...


def _create_generator_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
