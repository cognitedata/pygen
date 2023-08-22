from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from shop_pydantic_v1.client.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from shop_pydantic_v1.client.data_classes._command_configs import CommandConfigApply

__all__ = ["Case", "CaseApply", "CaseList"]


class Case(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    arguments: Optional[str] = None
    bid: Optional[str] = None
    bid_history: list[str] = []
    commands: Optional[str] = None
    cut_files: list[str] = []
    end_time: Optional[datetime.datetime] = None
    name: Optional[str] = None
    run_status: Optional[str] = Field(None, alias="runStatus")
    scenario: Optional[str] = None
    start_time: Optional[datetime.datetime] = None


class CaseApply(DomainModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    arguments: Optional[str] = None
    bid: Optional[str] = None
    bid_history: list[str] = []
    commands: Optional[Union["CommandConfigApply", str]] = Field(None, repr=False)
    cut_files: list[str] = []
    end_time: Optional[datetime.datetime] = None
    name: str
    run_status: str
    scenario: Optional[str] = None
    start_time: datetime.datetime

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.arguments is not None:
            properties["arguments"] = self.arguments
        if self.bid is not None:
            properties["bid"] = self.bid
        if self.bid_history is not None:
            properties["bid_history"] = self.bid_history
        if self.commands is not None:
            properties["commands"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.commands if isinstance(self.commands, str) else self.commands.external_id,
            }
        if self.cut_files is not None:
            properties["cut_files"] = self.cut_files
        if self.end_time is not None:
            properties["end_time"] = self.end_time.isoformat()
        if self.name is not None:
            properties["name"] = self.name
        if self.run_status is not None:
            properties["runStatus"] = self.run_status
        if self.scenario is not None:
            properties["scenario"] = self.scenario
        if self.start_time is not None:
            properties["start_time"] = self.start_time.isoformat()
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "Case"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        if isinstance(self.commands, DomainModelApply):
            instances = self.commands._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class CaseList(TypeList[Case]):
    _NODE = Case
