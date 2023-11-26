from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from osdu_wells.client.data_classes import (
    AsIngestedCoordinates,
    AsIngestedCoordinatesApply,
    Features,
    FeaturesApply,
)
from osdu_wells.client.data_classes._as_ingested_coordinates import (
    _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD,
)
from osdu_wells.client.data_classes._features import (
    _FEATURES_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from .features_query import FeaturesQueryAPI


class AsIngestedCoordinatesQueryAPI(QueryAPI[T_DomainModelList]):
    def features(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> FeaturesQueryAPI[T_DomainModelList]:
        """Query along the feature edges of the as ingested coordinate.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            FeaturesQueryAPI: The query API for the feature.
        """
        from .features_query import FeaturesQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "AsIngestedCoordinates.features"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("features"),
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
                name=self._builder.next_name("feature"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[FeaturesApply],
                            list(_FEATURES_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=Features,
                max_retrieve_limit=-1,
            ),
        )
        return FeaturesQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_as_ingested_coordinate: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_as_ingested_coordinate: Whether to retrieve the as ingested coordinate or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_as_ingested_coordinate and not self._builder[-1].name.startswith("as_ingested_coordinate"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("as_ingested_coordinate"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[AsIngestedCoordinatesApply],
                                list(_ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=AsIngestedCoordinates,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
