from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    NameAliases,
    NameAliasesApply,
    NameAliasesList,
    NameAliasesApplyList,
    NameAliasesFields,
    NameAliasesTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._name_aliases import _NAMEALIASES_PROPERTIES_BY_FIELD


class NameAliasesAPI(TypeAPI[NameAliases, NameAliasesApply, NameAliasesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[NameAliasesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=NameAliases,
            class_apply_type=NameAliasesApply,
            class_list=NameAliasesList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, name_alias: NameAliasesApply | Sequence[NameAliasesApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) name aliases.

        Args:
            name_alias: Name alias or sequence of name aliases to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new name_alias:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import NameAliasesApply
                >>> client = OSDUClient()
                >>> name_alias = NameAliasesApply(external_id="my_name_alias", ...)
                >>> result = client.name_aliases.apply(name_alias)

        """
        if isinstance(name_alias, NameAliasesApply):
            instances = name_alias.to_instances_apply(self._view_by_write_class)
        else:
            instances = NameAliasesApplyList(name_alias).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more name alias.

        Args:
            external_id: External id of the name alias to delete.
            space: The space where all the name alias are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete name_alias by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.name_aliases.delete("my_name_alias")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> NameAliases:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> NameAliasesList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> NameAliases | NameAliasesList:
        """Retrieve one or more name aliases by id(s).

        Args:
            external_id: External id or list of external ids of the name aliases.
            space: The space where all the name aliases are located.

        Returns:
            The requested name aliases.

        Examples:

            Retrieve name_alias by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> name_alias = client.name_aliases.retrieve("my_name_alias")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: NameAliasesTextFields | Sequence[NameAliasesTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> NameAliasesList:
        """Search name aliases

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            alias_name: The alias name to filter on.
            alias_name_prefix: The prefix of the alias name to filter on.
            alias_name_type_id: The alias name type id to filter on.
            alias_name_type_id_prefix: The prefix of the alias name type id to filter on.
            definition_organisation_id: The definition organisation id to filter on.
            definition_organisation_id_prefix: The prefix of the definition organisation id to filter on.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of name aliases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results name aliases matching the query.

        Examples:

           Search for 'my_name_alias' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> name_aliases = client.name_aliases.search('my_name_alias')

        """
        filter_ = _create_filter(
            self._view_id,
            alias_name,
            alias_name_prefix,
            alias_name_type_id,
            alias_name_type_id_prefix,
            definition_organisation_id,
            definition_organisation_id_prefix,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _NAMEALIASES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: NameAliasesFields | Sequence[NameAliasesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: NameAliasesTextFields | Sequence[NameAliasesTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: NameAliasesFields | Sequence[NameAliasesFields] | None = None,
        group_by: NameAliasesFields | Sequence[NameAliasesFields] = None,
        query: str | None = None,
        search_properties: NameAliasesTextFields | Sequence[NameAliasesTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: NameAliasesFields | Sequence[NameAliasesFields] | None = None,
        group_by: NameAliasesFields | Sequence[NameAliasesFields] | None = None,
        query: str | None = None,
        search_property: NameAliasesTextFields | Sequence[NameAliasesTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            alias_name,
            alias_name_prefix,
            alias_name_type_id,
            alias_name_type_id_prefix,
            definition_organisation_id,
            definition_organisation_id_prefix,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _NAMEALIASES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: NameAliasesFields,
        interval: float,
        query: str | None = None,
        search_property: NameAliasesTextFields | Sequence[NameAliasesTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            alias_name,
            alias_name_prefix,
            alias_name_type_id,
            alias_name_type_id_prefix,
            definition_organisation_id,
            definition_organisation_id_prefix,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _NAMEALIASES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> NameAliasesList:
        """List/filter name aliases

        Args:
            alias_name: The alias name to filter on.
            alias_name_prefix: The prefix of the alias name to filter on.
            alias_name_type_id: The alias name type id to filter on.
            alias_name_type_id_prefix: The prefix of the alias name type id to filter on.
            definition_organisation_id: The definition organisation id to filter on.
            definition_organisation_id_prefix: The prefix of the definition organisation id to filter on.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of name aliases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested name aliases

        Examples:

            List name aliases and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> name_aliases = client.name_aliases.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            alias_name,
            alias_name_prefix,
            alias_name_type_id,
            alias_name_type_id_prefix,
            definition_organisation_id,
            definition_organisation_id_prefix,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
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
    if alias_name and isinstance(alias_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AliasName"), value=alias_name))
    if alias_name and isinstance(alias_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AliasName"), values=alias_name))
    if alias_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("AliasName"), value=alias_name_prefix))
    if alias_name_type_id and isinstance(alias_name_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AliasNameTypeID"), value=alias_name_type_id))
    if alias_name_type_id and isinstance(alias_name_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AliasNameTypeID"), values=alias_name_type_id))
    if alias_name_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("AliasNameTypeID"), value=alias_name_type_id_prefix))
    if definition_organisation_id and isinstance(definition_organisation_id, str):
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
    if effective_date_time and isinstance(effective_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time))
    if effective_date_time and isinstance(effective_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EffectiveDateTime"), values=effective_date_time))
    if effective_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time_prefix)
        )
    if termination_date_time and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
