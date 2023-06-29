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
        node_data = dm.NodeOrEdgeData(
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
        )
        if self.commands:
            node_data.properties["commands"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.commands if isinstance(self.commands, str) else self.commands.external_id,
            }
        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[node_data],
        )
        nodes = [this_node]
        edges = []

        if self.commands is not None:
            if isinstance(self.commands, CircularModelApply):
                instances = self.commands._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)


class CaseList(TypeList[Case]):
    _NODE = Case
