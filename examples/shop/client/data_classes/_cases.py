from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import CircularModelApply, DomainModel, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._command_configs import Command_ConfigApply

__all__ = ["Case", "CaseApply", "CaseList"]


class Case(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: Optional[str] = None
    scenario: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    commands: Optional[str] = None
    cut_files: list[str] = None
    bid: Optional[str] = None
    bid_history: list[str] = None
    run_status: Optional[str] = Field(None, alias="runStatus")
    arguments: Optional[str] = None


class CaseApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: str
    scenario: Optional[str] = None
    start_time: datetime
    end_time: Optional[datetime] = None
    commands: Optional[Union[str, "Command_ConfigApply"]] = None
    cut_files: list[str] = None
    bid: Optional[str] = None
    bid_history: list[str] = None
    run_status: str
    arguments: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("IntegrationTestsImmutable", "Case"),
                    properties={
                        "name": self.name,
                        "scenario": self.scenario,
                        "start_time": self.start_time,
                        "end_time": self.end_time,
                        "cut_files": self.cut_files,
                        "bid": self.bid,
                        "bid_history": self.bid_history,
                        "runStatus": self.run_status,
                        "arguments": self.arguments,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []

        if self.command is not None:
            edge = self._create_command_edge(self.command)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(self.command, CircularModelApply):
                instances = self.command._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_command_edge(self, command: Union[str, "CommandApply"]) -> dm.EdgeApply:
        if isinstance(command, str):
            end_node_ext_id = command
        elif isinstance(command, CircularModelApply):
            end_node_ext_id = command.external_id
        else:
            raise TypeError(f"Expected str or CommandApply, got {type(command)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Case.commands"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )


class CaseList(TypeList[Case]):
    _NODE = Case
