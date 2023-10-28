from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "NameAliases",
    "NameAliasesApply",
    "NameAliasesList",
    "NameAliasesApplyList",
    "NameAliasesFields",
    "NameAliasesTextFields",
]


NameAliasesTextFields = Literal[
    "alias_name", "alias_name_type_id", "definition_organisation_id", "effective_date_time", "termination_date_time"
]
NameAliasesFields = Literal[
    "alias_name", "alias_name_type_id", "definition_organisation_id", "effective_date_time", "termination_date_time"
]

_NAMEALIASES_PROPERTIES_BY_FIELD = {
    "alias_name": "AliasName",
    "alias_name_type_id": "AliasNameTypeID",
    "definition_organisation_id": "DefinitionOrganisationID",
    "effective_date_time": "EffectiveDateTime",
    "termination_date_time": "TerminationDateTime",
}


class NameAliases(DomainModel):
    space: str = "IntegrationTestsImmutable"
    alias_name: Optional[str] = Field(None, alias="AliasName")
    alias_name_type_id: Optional[str] = Field(None, alias="AliasNameTypeID")
    definition_organisation_id: Optional[str] = Field(None, alias="DefinitionOrganisationID")
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> NameAliasesApply:
        return NameAliasesApply(
            external_id=self.external_id,
            alias_name=self.alias_name,
            alias_name_type_id=self.alias_name_type_id,
            definition_organisation_id=self.definition_organisation_id,
            effective_date_time=self.effective_date_time,
            termination_date_time=self.termination_date_time,
        )


class NameAliasesApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    alias_name: Optional[str] = None
    alias_name_type_id: Optional[str] = None
    definition_organisation_id: Optional[str] = None
    effective_date_time: Optional[str] = None
    termination_date_time: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.alias_name is not None:
            properties["AliasName"] = self.alias_name
        if self.alias_name_type_id is not None:
            properties["AliasNameTypeID"] = self.alias_name_type_id
        if self.definition_organisation_id is not None:
            properties["DefinitionOrganisationID"] = self.definition_organisation_id
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "NameAliases"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class NameAliasesList(TypeList[NameAliases]):
    _NODE = NameAliases

    def as_apply(self) -> NameAliasesApplyList:
        return NameAliasesApplyList([node.as_apply() for node in self.data])


class NameAliasesApplyList(TypeApplyList[NameAliasesApply]):
    _NODE = NameAliasesApply
