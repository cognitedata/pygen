from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from osdu_wells.client.data_classes import (
    WellboreTrajectoryData,
    WellboreTrajectoryDataApply,
    Artefacts,
    ArtefactsApply,
    AvailableTrajectoryStationProperties,
    AvailableTrajectoryStationPropertiesApply,
    GeoContexts,
    GeoContextsApply,
    LineageAssertions,
    LineageAssertionsApply,
    NameAliases,
    NameAliasesApply,
    TechnicalAssurances,
    TechnicalAssurancesApply,
)
from osdu_wells.client.data_classes._wellbore_trajectory_data import (
    _WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD,
)
from osdu_wells.client.data_classes._artefacts import (
    _ARTEFACTS_PROPERTIES_BY_FIELD,
)
from osdu_wells.client.data_classes._available_trajectory_station_properties import (
    _AVAILABLETRAJECTORYSTATIONPROPERTIES_PROPERTIES_BY_FIELD,
)
from osdu_wells.client.data_classes._geo_contexts import (
    _GEOCONTEXTS_PROPERTIES_BY_FIELD,
)
from osdu_wells.client.data_classes._lineage_assertions import (
    _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD,
)
from osdu_wells.client.data_classes._name_aliases import (
    _NAMEALIASES_PROPERTIES_BY_FIELD,
)
from osdu_wells.client.data_classes._technical_assurances import (
    _TECHNICALASSURANCES_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from .artefacts_query import ArtefactsQueryAPI
    from .available_trajectory_station_properties_query import AvailableTrajectoryStationPropertiesQueryAPI
    from .geo_contexts_query import GeoContextsQueryAPI
    from .lineage_assertions_query import LineageAssertionsQueryAPI
    from .name_aliases_query import NameAliasesQueryAPI
    from .technical_assurances_query import TechnicalAssurancesQueryAPI


class WellboreTrajectoryDataQueryAPI(QueryAPI[T_DomainModelList]):
    def artefacts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> ArtefactsQueryAPI[T_DomainModelList]:
        """Query along the artefact edges of the wellbore trajectory datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ArtefactsQueryAPI: The query API for the artefact.
        """
        from .artefacts_query import ArtefactsQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.Artefacts"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("artefacts"),
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
                name=self._builder.next_name("artefact"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[ArtefactsApply],
                            list(_ARTEFACTS_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=Artefacts,
                max_retrieve_limit=-1,
            ),
        )
        return ArtefactsQueryAPI(self._client, self._builder, self._view_by_write_class)

    def available_trajectory_station_properties(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> AvailableTrajectoryStationPropertiesQueryAPI[T_DomainModelList]:
        """Query along the available trajectory station property edges of the wellbore trajectory datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            AvailableTrajectoryStationPropertiesQueryAPI: The query API for the available trajectory station property.
        """
        from .available_trajectory_station_properties_query import AvailableTrajectoryStationPropertiesQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference(
                "IntegrationTestsImmutable", "WellboreTrajectoryData.AvailableTrajectoryStationProperties"
            ),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("available_trajectory_station_properties"),
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
                name=self._builder.next_name("available_trajectory_station_property"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[AvailableTrajectoryStationPropertiesApply],
                            list(_AVAILABLETRAJECTORYSTATIONPROPERTIES_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=AvailableTrajectoryStationProperties,
                max_retrieve_limit=-1,
            ),
        )
        return AvailableTrajectoryStationPropertiesQueryAPI(self._client, self._builder, self._view_by_write_class)

    def geo_contexts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> GeoContextsQueryAPI[T_DomainModelList]:
        """Query along the geo context edges of the wellbore trajectory datum.

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
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.GeoContexts"),
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

    def lineage_assertions(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> LineageAssertionsQueryAPI[T_DomainModelList]:
        """Query along the lineage assertion edges of the wellbore trajectory datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            LineageAssertionsQueryAPI: The query API for the lineage assertion.
        """
        from .lineage_assertions_query import LineageAssertionsQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.LineageAssertions"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("lineage_assertions"),
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
                name=self._builder.next_name("lineage_assertion"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[LineageAssertionsApply],
                            list(_LINEAGEASSERTIONS_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=LineageAssertions,
                max_retrieve_limit=-1,
            ),
        )
        return LineageAssertionsQueryAPI(self._client, self._builder, self._view_by_write_class)

    def name_aliases(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> NameAliasesQueryAPI[T_DomainModelList]:
        """Query along the name alias edges of the wellbore trajectory datum.

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
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.NameAliases"),
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
        """Query along the technical assurance edges of the wellbore trajectory datum.

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
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.TechnicalAssurances"),
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

    def query(
        self,
        retrieve_wellbore_trajectory_datum: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_wellbore_trajectory_datum: Whether to retrieve the wellbore trajectory datum or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_wellbore_trajectory_datum and not self._builder[-1].name.startswith("wellbore_trajectory_datum"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("wellbore_trajectory_datum"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[WellboreTrajectoryDataApply],
                                list(_WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=WellboreTrajectoryData,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
