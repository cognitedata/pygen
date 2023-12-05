from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from osdu_wells.client.data_classes import (
    DomainModelApply,
    WellData,
    WellDataApply,
    SpatialLocation,
    SpatialLocationApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

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
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_write_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("well_datum"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[WellDataApply], ["*"])]),
                result_cls=WellData,
                max_retrieve_limit=limit,
            )
        )

    def facility_events(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_location: bool = False,
    ) -> FacilityEventsQueryAPI[T_DomainModelList]:
        """Query along the facility event edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility event edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_location: Whether to retrieve the spatial location for each well datum or not.

        Returns:
            FacilityEventsQueryAPI: The query API for the facility event.
        """
        from .facility_events_query import FacilityEventsQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_location:
            self._query_append_spatial_location(from_)
        return FacilityEventsQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def facility_operators(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_location: bool = False,
    ) -> FacilityOperatorsQueryAPI[T_DomainModelList]:
        """Query along the facility operator edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility operator edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_location: Whether to retrieve the spatial location for each well datum or not.

        Returns:
            FacilityOperatorsQueryAPI: The query API for the facility operator.
        """
        from .facility_operators_query import FacilityOperatorsQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_location:
            self._query_append_spatial_location(from_)
        return FacilityOperatorsQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def facility_specifications(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_location: bool = False,
    ) -> FacilitySpecificationsQueryAPI[T_DomainModelList]:
        """Query along the facility specification edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility specification edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_location: Whether to retrieve the spatial location for each well datum or not.

        Returns:
            FacilitySpecificationsQueryAPI: The query API for the facility specification.
        """
        from .facility_specifications_query import FacilitySpecificationsQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_location:
            self._query_append_spatial_location(from_)
        return FacilitySpecificationsQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def facility_states(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_location: bool = False,
    ) -> FacilityStatesQueryAPI[T_DomainModelList]:
        """Query along the facility state edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility state edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_location: Whether to retrieve the spatial location for each well datum or not.

        Returns:
            FacilityStatesQueryAPI: The query API for the facility state.
        """
        from .facility_states_query import FacilityStatesQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_location:
            self._query_append_spatial_location(from_)
        return FacilityStatesQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def geo_contexts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_location: bool = False,
    ) -> GeoContextsQueryAPI[T_DomainModelList]:
        """Query along the geo context edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of geo context edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_location: Whether to retrieve the spatial location for each well datum or not.

        Returns:
            GeoContextsQueryAPI: The query API for the geo context.
        """
        from .geo_contexts_query import GeoContextsQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_location:
            self._query_append_spatial_location(from_)
        return GeoContextsQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def historical_interests(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_location: bool = False,
    ) -> HistoricalInterestsQueryAPI[T_DomainModelList]:
        """Query along the historical interest edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of historical interest edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_location: Whether to retrieve the spatial location for each well datum or not.

        Returns:
            HistoricalInterestsQueryAPI: The query API for the historical interest.
        """
        from .historical_interests_query import HistoricalInterestsQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_location:
            self._query_append_spatial_location(from_)
        return HistoricalInterestsQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def name_aliases(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_location: bool = False,
    ) -> NameAliasesQueryAPI[T_DomainModelList]:
        """Query along the name alias edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of name alias edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_location: Whether to retrieve the spatial location for each well datum or not.

        Returns:
            NameAliasesQueryAPI: The query API for the name alias.
        """
        from .name_aliases_query import NameAliasesQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_location:
            self._query_append_spatial_location(from_)
        return NameAliasesQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def technical_assurances(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_location: bool = False,
    ) -> TechnicalAssurancesQueryAPI[T_DomainModelList]:
        """Query along the technical assurance edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of technical assurance edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_location: Whether to retrieve the spatial location for each well datum or not.

        Returns:
            TechnicalAssurancesQueryAPI: The query API for the technical assurance.
        """
        from .technical_assurances_query import TechnicalAssurancesQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_location:
            self._query_append_spatial_location(from_)
        return TechnicalAssurancesQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def vertical_measurements(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_location: bool = False,
    ) -> VerticalMeasurementsQueryAPI[T_DomainModelList]:
        """Query along the vertical measurement edges of the well datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of vertical measurement edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_location: Whether to retrieve the spatial location for each well datum or not.

        Returns:
            VerticalMeasurementsQueryAPI: The query API for the vertical measurement.
        """
        from .vertical_measurements_query import VerticalMeasurementsQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_location:
            self._query_append_spatial_location(from_)
        return VerticalMeasurementsQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
        retrieve_spatial_location: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_spatial_location: Whether to retrieve the spatial location for each well datum or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_spatial_location:
            self._query_append_spatial_location(from_)
        return self._query()

    def _query_append_spatial_location(self, from_: str) -> None:
        view_id = self._view_by_write_class[SpatialLocationApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("spatial_location"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[WellDataApply].as_property_ref("spatial_location"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=SpatialLocation,
            ),
        )
