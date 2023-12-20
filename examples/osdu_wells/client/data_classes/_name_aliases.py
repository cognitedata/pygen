from __future__ import annotations

from typing import Literal, Optional

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
    """This represents the reading version of name alias.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the name alias.
        alias_name: The alias name field.
        alias_name_type_id: The alias name type id field.
        definition_organisation_id: The definition organisation id field.
        effective_date_time: The effective date time field.
        termination_date_time: The termination date time field.
        created_time: The created time of the name alias node.
        last_updated_time: The last updated time of the name alias node.
        deleted_time: If present, the deleted time of the name alias node.
        version: The version of the name alias node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    alias_name: Optional[str] = Field(None, alias="AliasName")
    alias_name_type_id: Optional[str] = Field(None, alias="AliasNameTypeID")
    definition_organisation_id: Optional[str] = Field(None, alias="DefinitionOrganisationID")
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> NameAliasesApply:
        """Convert this read version of name alias to the writing version."""
        return NameAliasesApply(
            space=self.space,
            external_id=self.external_id,
            alias_name=self.alias_name,
            alias_name_type_id=self.alias_name_type_id,
            definition_organisation_id=self.definition_organisation_id,
            effective_date_time=self.effective_date_time,
            termination_date_time=self.termination_date_time,
        )


class NameAliasesApply(DomainModelApply):
    """This represents the writing version of name alias.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the name alias.
        alias_name: The alias name field.
        alias_name_type_id: The alias name type id field.
        definition_organisation_id: The definition organisation id field.
        effective_date_time: The effective date time field.
        termination_date_time: The termination date time field.
        existing_version: Fail the ingestion request if the name alias version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    alias_name: Optional[str] = Field(None, alias="AliasName")
    alias_name_type_id: Optional[str] = Field(None, alias="AliasNameTypeID")
    definition_organisation_id: Optional[str] = Field(None, alias="DefinitionOrganisationID")
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "NameAliases", "b0ef9b17280885"
        )

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

        return resources


class NameAliasesList(DomainModelList[NameAliases]):
    """List of name aliases in the read version."""

    _INSTANCE = NameAliases

    def as_apply(self) -> NameAliasesApplyList:
        """Convert these read versions of name alias to the writing versions."""
        return NameAliasesApplyList([node.as_apply() for node in self.data])


class NameAliasesApplyList(DomainModelApplyList[NameAliasesApply]):
    """List of name aliases in the writing version."""

    _INSTANCE = NameAliasesApply


def _create_name_alias_filter(
    view_id: dm.ViewId,
    alias_name: str | list[str] | None = None,
    alias_name_prefix: str | None = None,
    alias_name_type_id: str | list[str] | None = None,
    alias_name_type_id_prefix: str | None = None,
    definition_organisation_id: str | list[str] | None = None,
    definition_organisation_id_prefix: str | None = None,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if alias_name is not None and isinstance(alias_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AliasName"), value=alias_name))
    if alias_name and isinstance(alias_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AliasName"), values=alias_name))
    if alias_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("AliasName"), value=alias_name_prefix))
    if alias_name_type_id is not None and isinstance(alias_name_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AliasNameTypeID"), value=alias_name_type_id))
    if alias_name_type_id and isinstance(alias_name_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AliasNameTypeID"), values=alias_name_type_id))
    if alias_name_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("AliasNameTypeID"), value=alias_name_type_id_prefix))
    if definition_organisation_id is not None and isinstance(definition_organisation_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("DefinitionOrganisationID"), value=definition_organisation_id)
        )
    if definition_organisation_id and isinstance(definition_organisation_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("DefinitionOrganisationID"), values=definition_organisation_id)
        )
    if definition_organisation_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("DefinitionOrganisationID"), value=definition_organisation_id_prefix
            )
        )
    if effective_date_time is not None and isinstance(effective_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time))
    if effective_date_time and isinstance(effective_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EffectiveDateTime"), values=effective_date_time))
    if effective_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time_prefix)
        )
    if termination_date_time is not None and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
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
