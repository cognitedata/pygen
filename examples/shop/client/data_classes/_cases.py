from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import CircularModelApply, DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._command_configs import CommandConfigApply

__all__ = ["Case", "CaseApply", "CaseList"]


class Case(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    argument: Optional[str] = Field(None, alias="arguments")
    bid: Optional[str] = None
    bid_histories: list[str] = Field([], alias="bid_history")
    command: Optional[str] = Field(None, alias="commands")
    cut_files: list[str] = []
    end_time: Optional[datetime] = None
    name: Optional[str] = None
    run_status: Optional[str] = Field(None, alias="runStatus")
    scenario: Optional[str] = None
    start_time: Optional[datetime] = None


class CaseApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    argument: Optional[str] = None
    bid: Optional[str] = None
    bid_histories: list[str] = []
    command: Optional[Union[str, "CommandConfigApply"]] = None
    cut_files: list[str] = []
    end_time: Optional[datetime] = None
    name: str
    run_status: str
    scenario: Optional[str] = None
    start_time: datetime

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("IntegrationTestsImmutable", "Case"),
            properties={
                "arguments": self.argument,
                "bid": self.bid,
                "bid_history": self.bid_histories,
                "commands": {
                    "space": "IntegrationTestsImmutable",
                    "externalId": self.command if isinstance(self.command, str) else self.command.external_id,
                },
                "cut_files": self.cut_files,
                "end_time": self.end_time.isoformat(),
                "name": self.name,
                "runStatus": self.run_status,
                "scenario": self.scenario,
                "start_time": self.start_time.isoformat(),
            },
        )
        sources.append(source)

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=sources,
        )
        nodes = [this_node]
        edges = []

        if isinstance(self.command, DomainModelApply):
            instances = self.command._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)


class CaseList(TypeList[Case]):
    _NODE = Case
