from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from shop.client.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from shop.client.data_classes._command_configs import CommandConfigApply

__all__ = ["Case", "CaseApply", "CaseList"]


class Case(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    arguments: Optional[str] = None
    bid: Optional[str] = None
    bid_history: list[str] = []
    commands: Optional[str] = None
    cut_files: list[str] = []
    end_time: Optional[datetime] = None
    name: Optional[str] = None
    run_status: Optional[str] = Field(None, alias="runStatus")
    scenario: Optional[str] = None
    start_time: Optional[datetime] = None


class CaseApply(DomainModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    arguments: Optional[str] = None
    bid: Optional[str] = None
    bid_history: list[str] = []
    commands: Optional[Union["CommandConfigApply", str]] = Field(None, repr=False)
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
                "arguments": self.arguments,
                "bid": self.bid,
                "bid_history": self.bid_history,
                "commands": {
                    "space": "IntegrationTestsImmutable",
                    "externalId": self.commands if isinstance(self.commands, str) else self.commands.external_id,
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

        if isinstance(self.commands, DomainModelApply):
            instances = self.commands._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)


class CaseList(TypeList[Case]):
    _NODE = Case
