from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._command_config import CommandConfig, CommandConfigApply


__all__ = ["Case", "CaseApply", "CaseList", "CaseApplyList", "CaseFields", "CaseTextFields"]


CaseTextFields = Literal["arguments", "bid", "bid_history", "cut_files", "name", "run_status", "scenario"]
CaseFields = Literal[
    "arguments", "bid", "bid_history", "cut_files", "end_time", "name", "run_status", "scenario", "start_time"
]

_CASE_PROPERTIES_BY_FIELD = {
    "arguments": "arguments",
    "bid": "bid",
    "bid_history": "bid_history",
    "cut_files": "cut_files",
    "end_time": "end_time",
    "name": "name",
    "run_status": "runStatus",
    "scenario": "scenario",
    "start_time": "start_time",
}


class Case(DomainModel):
    """This represents the reading version of case.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the case.
        arguments: The argument field.
        bid: The bid field.
        bid_history: The bid history field.
        commands: The command field.
        cut_files: The cut file field.
        end_time: The end time field.
        name: The name field.
        run_status: The run status field.
        scenario: The scenario field.
        start_time: The start time field.
        created_time: The created time of the case node.
        last_updated_time: The last updated time of the case node.
        deleted_time: If present, the deleted time of the case node.
        version: The version of the case node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    arguments: Optional[str] = None
    bid: Union[str, None] = None
    bid_history: Optional[list[str]] = None
    commands: Union[CommandConfig, str, dm.NodeId, None] = Field(None, repr=False)
    cut_files: Optional[list[str]] = None
    end_time: Optional[datetime.datetime] = None
    name: Optional[str] = None
    run_status: Optional[str] = Field(None, alias="runStatus")
    scenario: Optional[str] = None
    start_time: Optional[datetime.datetime] = None

    def as_apply(self) -> CaseApply:
        """Convert this read version of case to the writing version."""
        return CaseApply(
            space=self.space,
            external_id=self.external_id,
            arguments=self.arguments,
            bid=self.bid,
            bid_history=self.bid_history,
            commands=self.commands.as_apply() if isinstance(self.commands, DomainModel) else self.commands,
            cut_files=self.cut_files,
            end_time=self.end_time,
            name=self.name,
            run_status=self.run_status,
            scenario=self.scenario,
            start_time=self.start_time,
        )


class CaseApply(DomainModelApply):
    """This represents the writing version of case.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the case.
        arguments: The argument field.
        bid: The bid field.
        bid_history: The bid history field.
        commands: The command field.
        cut_files: The cut file field.
        end_time: The end time field.
        name: The name field.
        run_status: The run status field.
        scenario: The scenario field.
        start_time: The start time field.
        existing_version: Fail the ingestion request if the case version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    arguments: Optional[str] = None
    bid: Union[str, None] = None
    bid_history: Optional[list[str]] = None
    commands: Union[CommandConfigApply, str, dm.NodeId, None] = Field(None, repr=False)
    cut_files: Optional[list[str]] = None
    end_time: Optional[datetime.datetime] = None
    name: str
    run_status: str = Field(alias="runStatus")
    scenario: Optional[str] = None
    start_time: datetime.datetime

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Case", "366b75cc4e699f"
        )

        properties = {}

        if self.arguments is not None:
            properties["arguments"] = self.arguments

        if self.bid is not None:
            properties["bid"] = self.bid

        if self.bid_history is not None:
            properties["bid_history"] = self.bid_history

        if self.commands is not None:
            properties["commands"] = {
                "space": self.space if isinstance(self.commands, str) else self.commands.space,
                "externalId": self.commands if isinstance(self.commands, str) else self.commands.external_id,
            }

        if self.cut_files is not None:
            properties["cut_files"] = self.cut_files

        if self.end_time is not None:
            properties["end_time"] = self.end_time.isoformat(timespec="milliseconds")

        if self.name is not None:
            properties["name"] = self.name

        if self.run_status is not None:
            properties["runStatus"] = self.run_status

        if self.scenario is not None:
            properties["scenario"] = self.scenario

        if self.start_time is not None:
            properties["start_time"] = self.start_time.isoformat(timespec="milliseconds")

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

        if isinstance(self.commands, DomainModelApply):
            other_resources = self.commands._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class CaseList(DomainModelList[Case]):
    """List of cases in the read version."""

    _INSTANCE = Case

    def as_apply(self) -> CaseApplyList:
        """Convert these read versions of case to the writing versions."""
        return CaseApplyList([node.as_apply() for node in self.data])


class CaseApplyList(DomainModelApplyList[CaseApply]):
    """List of cases in the writing version."""

    _INSTANCE = CaseApply


def _create_case_filter(
    view_id: dm.ViewId,
    arguments: str | list[str] | None = None,
    arguments_prefix: str | None = None,
    commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    run_status: str | list[str] | None = None,
    run_status_prefix: str | None = None,
    scenario: str | list[str] | None = None,
    scenario_prefix: str | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if arguments is not None and isinstance(arguments, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("arguments"), value=arguments))
    if arguments and isinstance(arguments, list):
        filters.append(dm.filters.In(view_id.as_property_ref("arguments"), values=arguments))
    if arguments_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("arguments"), value=arguments_prefix))
    if commands and isinstance(commands, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("commands"),
                value={"space": "IntegrationTestsImmutable", "externalId": commands},
            )
        )
    if commands and isinstance(commands, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("commands"), value={"space": commands[0], "externalId": commands[1]}
            )
        )
    if commands and isinstance(commands, list) and isinstance(commands[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("commands"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in commands],
            )
        )
    if commands and isinstance(commands, list) and isinstance(commands[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("commands"),
                values=[{"space": item[0], "externalId": item[1]} for item in commands],
            )
        )
    if min_end_time or max_end_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("end_time"),
                gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None,
                lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None,
            )
        )
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if run_status is not None and isinstance(run_status, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("runStatus"), value=run_status))
    if run_status and isinstance(run_status, list):
        filters.append(dm.filters.In(view_id.as_property_ref("runStatus"), values=run_status))
    if run_status_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("runStatus"), value=run_status_prefix))
    if scenario is not None and isinstance(scenario, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value=scenario))
    if scenario and isinstance(scenario, list):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=scenario))
    if scenario_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("scenario"), value=scenario_prefix))
    if min_start_time or max_start_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("start_time"),
                gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None,
                lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
