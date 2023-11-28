from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    NameAliases,
    NameAliasesApply,
    NameAliasesFields,
    NameAliasesList,
    NameAliasesApplyList,
    NameAliasesTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._name_aliases import (
    _NAMEALIASES_PROPERTIES_BY_FIELD,
    _create_name_alias_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .name_aliases_query import NameAliasesQueryAPI


class NameAliasesAPI(NodeAPI[NameAliases, NameAliasesApply, NameAliasesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[NameAliasesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=NameAliases,
            class_apply_type=NameAliasesApply,
            class_list=NameAliasesList,
            class_apply_list=NameAliasesApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
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
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> NameAliasesQueryAPI[NameAliasesList]:
        """Query starting at name aliases.

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
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for name aliases.

        """
        filter_ = _create_name_alias_filter(
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
        builder = QueryBuilder(
            NameAliasesList,
            [
                QueryStep(
                    name="name_alias",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_NAMEALIASES_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=NameAliases,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return NameAliasesQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, name_alias: NameAliasesApply | Sequence[NameAliasesApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) name aliases.

        Args:
            name_alias: Name alias or sequence of name aliases to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new name_alias:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import NameAliasesApply
                >>> client = OSDUClient()
                >>> name_alias = NameAliasesApply(external_id="my_name_alias", ...)
                >>> result = client.name_aliases.apply(name_alias)

        """
        return self._apply(name_alias, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more name alias.

        Args:
            external_id: External id of the name alias to delete.
            space: The space where all the name alias are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete name_alias by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.name_aliases.delete("my_name_alias")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> NameAliases | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> NameAliasesList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> NameAliases | NameAliasesList | None:
        """Retrieve one or more name aliases by id(s).

        Args:
            external_id: External id or list of external ids of the name aliases.
            space: The space where all the name aliases are located.

        Returns:
            The requested name aliases.

        Examples:

            Retrieve name_alias by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> name_alias = client.name_aliases.retrieve("my_name_alias")

        """
        return self._retrieve(external_id, space)

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
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results name aliases matching the query.

        Examples:

           Search for 'my_name_alias' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> name_aliases = client.name_aliases.search('my_name_alias')

        """
        filter_ = _create_name_alias_filter(
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
        """Aggregate data across name aliases

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
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
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count name aliases in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.name_aliases.aggregate("count", space="my_space")

        """

        filter_ = _create_name_alias_filter(
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
        """Produces histograms for name aliases

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
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
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_name_alias_filter(
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
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested name aliases

        Examples:

            List name aliases and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> name_aliases = client.name_aliases.list(limit=5)

        """
        filter_ = _create_name_alias_filter(
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
