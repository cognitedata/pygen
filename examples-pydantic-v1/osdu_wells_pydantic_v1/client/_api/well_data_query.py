from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from osdu_wells_pydantic_v1.client.data_classes import (
    WellData,
    WellDataApply,
    FacilityEvents,
    FacilityEventsApply,
    FacilityOperators,
    FacilityOperatorsApply,
    FacilitySpecifications,
    FacilitySpecificationsApply,
    FacilityStates,
    FacilityStatesApply,
    GeoContexts,
    GeoContextsApply,
    HistoricalInterests,
    HistoricalInterestsApply,
    NameAliases,
    NameAliasesApply,
    TechnicalAssurances,
    TechnicalAssurancesApply,
    VerticalMeasurements,
    VerticalMeasurementsApply,
)
from osdu_wells_pydantic_v1.client.data_classes._well_data import (
    _WELLDATA_PROPERTIES_BY_FIELD,
)
from osdu_wells_pydantic_v1.client.data_classes._facility_events import (
    _FACILITYEVENTS_PROPERTIES_BY_FIELD,
)
from osdu_wells_pydantic_v1.client.data_classes._facility_operators import (
    _FACILITYOPERATORS_PROPERTIES_BY_FIELD,
)
from osdu_wells_pydantic_v1.client.data_classes._facility_specifications import (
    _FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD,
)
from osdu_wells_pydantic_v1.client.data_classes._facility_states import (
    _FACILITYSTATES_PROPERTIES_BY_FIELD,
)
from osdu_wells_pydantic_v1.client.data_classes._geo_contexts import (
    _GEOCONTEXTS_PROPERTIES_BY_FIELD,
)
from osdu_wells_pydantic_v1.client.data_classes._historical_interests import (
    _HISTORICALINTERESTS_PROPERTIES_BY_FIELD,
)
from osdu_wells_pydantic_v1.client.data_classes._name_aliases import (
    _NAMEALIASES_PROPERTIES_BY_FIELD,
)
from osdu_wells_pydantic_v1.client.data_classes._technical_assurances import (
    _TECHNICALASSURANCES_PROPERTIES_BY_FIELD,
)
from osdu_wells_pydantic_v1.client.data_classes._vertical_measurements import (
    _VERTICALMEASUREMENTS_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from .facility_events_query import FacilityEventsQueryAPI
    from .facility_operators_query import FacilityOperatorsQueryAPI
    from .facility_specifications_query import FacilitySpecificationsQueryAPI
    from .facility_states_query import FacilityStatesQueryAPI
    from .geo_contexts_query import GeoContextsQueryAPI
    from .historical_interests_query import HistoricalInterestsQueryAPI
    from .name_aliases_query import NameAliasesQueryAPI
    from .technical_assurances_query import TechnicalAssurancesQueryAPI
    from .vertical_measurements_query import VerticalMeasurementsQueryAPI


class WellDataQueryAPI(QueryAPI[T_DomainModelList]):
    def facility_events(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> FacilityEventsQueryAPI[T_DomainModelList]:
        """Query along the facility event edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            FacilityEventsQueryAPI: The query API for the facility event.
        """
        from .facility_events_query import FacilityEventsQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilityEvents"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("facility_events"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("facility_event"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[FacilityEventsApply],
                            list(_FACILITYEVENTS_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=FacilityEvents,
                max_retrieve_limit=-1,
            ),
        )
        return FacilityEventsQueryAPI(self._client, self._builder, self._view_by_write_class)

    def facility_operators(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> FacilityOperatorsQueryAPI[T_DomainModelList]:
        """Query along the facility operator edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            FacilityOperatorsQueryAPI: The query API for the facility operator.
        """
        from .facility_operators_query import FacilityOperatorsQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilityOperators"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("facility_operators"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("facility_operator"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[FacilityOperatorsApply],
                            list(_FACILITYOPERATORS_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=FacilityOperators,
                max_retrieve_limit=-1,
            ),
        )
        return FacilityOperatorsQueryAPI(self._client, self._builder, self._view_by_write_class)

    def facility_specifications(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> FacilitySpecificationsQueryAPI[T_DomainModelList]:
        """Query along the facility specification edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            FacilitySpecificationsQueryAPI: The query API for the facility specification.
        """
        from .facility_specifications_query import FacilitySpecificationsQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilitySpecifications"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("facility_specifications"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("facility_specification"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[FacilitySpecificationsApply],
                            list(_FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=FacilitySpecifications,
                max_retrieve_limit=-1,
            ),
        )
        return FacilitySpecificationsQueryAPI(self._client, self._builder, self._view_by_write_class)

    def facility_states(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> FacilityStatesQueryAPI[T_DomainModelList]:
        """Query along the facility state edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            FacilityStatesQueryAPI: The query API for the facility state.
        """
        from .facility_states_query import FacilityStatesQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilityStates"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("facility_states"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("facility_state"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[FacilityStatesApply],
                            list(_FACILITYSTATES_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=FacilityStates,
                max_retrieve_limit=-1,
            ),
        )
        return FacilityStatesQueryAPI(self._client, self._builder, self._view_by_write_class)

    def geo_contexts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> GeoContextsQueryAPI[T_DomainModelList]:
        """Query along the geo context edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            GeoContextsQueryAPI: The query API for the geo context.
        """
        from .geo_contexts_query import GeoContextsQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.GeoContexts"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("geo_contexts"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("geo_context"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[GeoContextsApply],
                            list(_GEOCONTEXTS_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=GeoContexts,
                max_retrieve_limit=-1,
            ),
        )
        return GeoContextsQueryAPI(self._client, self._builder, self._view_by_write_class)

    def historical_interests(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> HistoricalInterestsQueryAPI[T_DomainModelList]:
        """Query along the historical interest edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            HistoricalInterestsQueryAPI: The query API for the historical interest.
        """
        from .historical_interests_query import HistoricalInterestsQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.HistoricalInterests"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("historical_interests"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("historical_interest"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[HistoricalInterestsApply],
                            list(_HISTORICALINTERESTS_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=HistoricalInterests,
                max_retrieve_limit=-1,
            ),
        )
        return HistoricalInterestsQueryAPI(self._client, self._builder, self._view_by_write_class)

    def name_aliases(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> NameAliasesQueryAPI[T_DomainModelList]:
        """Query along the name alias edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            NameAliasesQueryAPI: The query API for the name alias.
        """
        from .name_aliases_query import NameAliasesQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.NameAliases"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("name_aliases"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("name_alias"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[NameAliasesApply],
                            list(_NAMEALIASES_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=NameAliases,
                max_retrieve_limit=-1,
            ),
        )
        return NameAliasesQueryAPI(self._client, self._builder, self._view_by_write_class)

    def technical_assurances(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> TechnicalAssurancesQueryAPI[T_DomainModelList]:
        """Query along the technical assurance edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            TechnicalAssurancesQueryAPI: The query API for the technical assurance.
        """
        from .technical_assurances_query import TechnicalAssurancesQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.TechnicalAssurances"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("technical_assurances"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("technical_assurance"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[TechnicalAssurancesApply],
                            list(_TECHNICALASSURANCES_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=TechnicalAssurances,
                max_retrieve_limit=-1,
            ),
        )
        return TechnicalAssurancesQueryAPI(self._client, self._builder, self._view_by_write_class)

    def vertical_measurements(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> VerticalMeasurementsQueryAPI[T_DomainModelList]:
        """Query along the vertical measurement edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            VerticalMeasurementsQueryAPI: The query API for the vertical measurement.
        """
        from .vertical_measurements_query import VerticalMeasurementsQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.VerticalMeasurements"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("vertical_measurements"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("vertical_measurement"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[VerticalMeasurementsApply],
                            list(_VERTICALMEASUREMENTS_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=VerticalMeasurements,
                max_retrieve_limit=-1,
            ),
        )
        return VerticalMeasurementsQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_well_datum: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_well_datum: Whether to retrieve the well datum or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_well_datum and not self._builder[-1].name.startswith("well_datum"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("well_datum"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[WellDataApply],
                                list(_WELLDATA_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=WellData,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
