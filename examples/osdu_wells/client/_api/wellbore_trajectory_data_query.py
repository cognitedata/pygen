from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from osdu_wells.client.data_classes import (
    DomainModelApply,
    WellboreTrajectoryData,
    WellboreTrajectoryDataApply,
    SpatialArea,
    SpatialAreaApply,
    SpatialPoint,
    SpatialPointApply,
    VerticalMeasurement,
    VerticalMeasurementApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .artefacts_query import ArtefactsQueryAPI
    from .available_trajectory_station_properties_query import AvailableTrajectoryStationPropertiesQueryAPI
    from .geo_contexts_query import GeoContextsQueryAPI
    from .lineage_assertions_query import LineageAssertionsQueryAPI
    from .name_aliases_query import NameAliasesQueryAPI
    from .technical_assurances_query import TechnicalAssurancesQueryAPI


class WellboreTrajectoryDataQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("wellbore_trajectory_datum"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_write_class[WellboreTrajectoryDataApply], ["*"])]
                ),
                result_cls=WellboreTrajectoryData,
                max_retrieve_limit=limit,
            )
        )

    def artefacts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_area: bool = False,
        retrieve_spatial_point: bool = False,
        retrieve_vertical_measurement: bool = False,
    ) -> ArtefactsQueryAPI[T_DomainModelList]:
        """Query along the artefact edges of the wellbore trajectory datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of artefact edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_area: Whether to retrieve the spatial area for each wellbore trajectory datum or not.
            retrieve_spatial_point: Whether to retrieve the spatial point for each wellbore trajectory datum or not.
            retrieve_vertical_measurement: Whether to retrieve the vertical measurement for each wellbore trajectory datum or not.

        Returns:
            ArtefactsQueryAPI: The query API for the artefact.
        """
        from .artefacts_query import ArtefactsQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_area:
            self._query_append_spatial_area(from_)
        if retrieve_spatial_point:
            self._query_append_spatial_point(from_)
        if retrieve_vertical_measurement:
            self._query_append_vertical_measurement(from_)
        return ArtefactsQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def available_trajectory_station_properties(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_area: bool = False,
        retrieve_spatial_point: bool = False,
        retrieve_vertical_measurement: bool = False,
    ) -> AvailableTrajectoryStationPropertiesQueryAPI[T_DomainModelList]:
        """Query along the available trajectory station property edges of the wellbore trajectory datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of available trajectory station property edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_area: Whether to retrieve the spatial area for each wellbore trajectory datum or not.
            retrieve_spatial_point: Whether to retrieve the spatial point for each wellbore trajectory datum or not.
            retrieve_vertical_measurement: Whether to retrieve the vertical measurement for each wellbore trajectory datum or not.

        Returns:
            AvailableTrajectoryStationPropertiesQueryAPI: The query API for the available trajectory station property.
        """
        from .available_trajectory_station_properties_query import AvailableTrajectoryStationPropertiesQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_area:
            self._query_append_spatial_area(from_)
        if retrieve_spatial_point:
            self._query_append_spatial_point(from_)
        if retrieve_vertical_measurement:
            self._query_append_vertical_measurement(from_)
        return AvailableTrajectoryStationPropertiesQueryAPI(
            self._client, self._builder, self._view_by_write_class, None, limit
        )

    def geo_contexts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_area: bool = False,
        retrieve_spatial_point: bool = False,
        retrieve_vertical_measurement: bool = False,
    ) -> GeoContextsQueryAPI[T_DomainModelList]:
        """Query along the geo context edges of the wellbore trajectory datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of geo context edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_area: Whether to retrieve the spatial area for each wellbore trajectory datum or not.
            retrieve_spatial_point: Whether to retrieve the spatial point for each wellbore trajectory datum or not.
            retrieve_vertical_measurement: Whether to retrieve the vertical measurement for each wellbore trajectory datum or not.

        Returns:
            GeoContextsQueryAPI: The query API for the geo context.
        """
        from .geo_contexts_query import GeoContextsQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_area:
            self._query_append_spatial_area(from_)
        if retrieve_spatial_point:
            self._query_append_spatial_point(from_)
        if retrieve_vertical_measurement:
            self._query_append_vertical_measurement(from_)
        return GeoContextsQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def lineage_assertions(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_area: bool = False,
        retrieve_spatial_point: bool = False,
        retrieve_vertical_measurement: bool = False,
    ) -> LineageAssertionsQueryAPI[T_DomainModelList]:
        """Query along the lineage assertion edges of the wellbore trajectory datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of lineage assertion edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_area: Whether to retrieve the spatial area for each wellbore trajectory datum or not.
            retrieve_spatial_point: Whether to retrieve the spatial point for each wellbore trajectory datum or not.
            retrieve_vertical_measurement: Whether to retrieve the vertical measurement for each wellbore trajectory datum or not.

        Returns:
            LineageAssertionsQueryAPI: The query API for the lineage assertion.
        """
        from .lineage_assertions_query import LineageAssertionsQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_area:
            self._query_append_spatial_area(from_)
        if retrieve_spatial_point:
            self._query_append_spatial_point(from_)
        if retrieve_vertical_measurement:
            self._query_append_vertical_measurement(from_)
        return LineageAssertionsQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def name_aliases(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_area: bool = False,
        retrieve_spatial_point: bool = False,
        retrieve_vertical_measurement: bool = False,
    ) -> NameAliasesQueryAPI[T_DomainModelList]:
        """Query along the name alias edges of the wellbore trajectory datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of name alias edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_area: Whether to retrieve the spatial area for each wellbore trajectory datum or not.
            retrieve_spatial_point: Whether to retrieve the spatial point for each wellbore trajectory datum or not.
            retrieve_vertical_measurement: Whether to retrieve the vertical measurement for each wellbore trajectory datum or not.

        Returns:
            NameAliasesQueryAPI: The query API for the name alias.
        """
        from .name_aliases_query import NameAliasesQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_area:
            self._query_append_spatial_area(from_)
        if retrieve_spatial_point:
            self._query_append_spatial_point(from_)
        if retrieve_vertical_measurement:
            self._query_append_vertical_measurement(from_)
        return NameAliasesQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def technical_assurances(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_spatial_area: bool = False,
        retrieve_spatial_point: bool = False,
        retrieve_vertical_measurement: bool = False,
    ) -> TechnicalAssurancesQueryAPI[T_DomainModelList]:
        """Query along the technical assurance edges of the wellbore trajectory datum.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of technical assurance edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_spatial_area: Whether to retrieve the spatial area for each wellbore trajectory datum or not.
            retrieve_spatial_point: Whether to retrieve the spatial point for each wellbore trajectory datum or not.
            retrieve_vertical_measurement: Whether to retrieve the vertical measurement for each wellbore trajectory datum or not.

        Returns:
            TechnicalAssurancesQueryAPI: The query API for the technical assurance.
        """
        from .technical_assurances_query import TechnicalAssurancesQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_spatial_area:
            self._query_append_spatial_area(from_)
        if retrieve_spatial_point:
            self._query_append_spatial_point(from_)
        if retrieve_vertical_measurement:
            self._query_append_vertical_measurement(from_)
        return TechnicalAssurancesQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
        retrieve_spatial_area: bool = False,
        retrieve_spatial_point: bool = False,
        retrieve_vertical_measurement: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_spatial_area: Whether to retrieve the spatial area for each wellbore trajectory datum or not.
            retrieve_spatial_point: Whether to retrieve the spatial point for each wellbore trajectory datum or not.
            retrieve_vertical_measurement: Whether to retrieve the vertical measurement for each wellbore trajectory datum or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_spatial_area:
            self._query_append_spatial_area(from_)
        if retrieve_spatial_point:
            self._query_append_spatial_point(from_)
        if retrieve_vertical_measurement:
            self._query_append_vertical_measurement(from_)
        return self._query()

    def _query_append_spatial_area(self, from_: str) -> None:
        view_id = self._view_by_write_class[SpatialAreaApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("spatial_area"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[WellboreTrajectoryDataApply].as_property_ref("spatial_area"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=SpatialArea,
            ),
        )

    def _query_append_spatial_point(self, from_: str) -> None:
        view_id = self._view_by_write_class[SpatialPointApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("spatial_point"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[WellboreTrajectoryDataApply].as_property_ref("spatial_point"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=SpatialPoint,
            ),
        )

    def _query_append_vertical_measurement(self, from_: str) -> None:
        view_id = self._view_by_write_class[VerticalMeasurementApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("vertical_measurement"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[WellboreTrajectoryDataApply].as_property_ref(
                        "vertical_measurement"
                    ),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=VerticalMeasurement,
            ),
        )
