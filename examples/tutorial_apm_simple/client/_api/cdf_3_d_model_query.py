from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from tutorial_apm_simple.client.data_classes import (
    CdfModel,
    CdfModelApply,
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
)
from tutorial_apm_simple.client.data_classes._cdf_3_d_model import (
    _CDFMODEL_PROPERTIES_BY_FIELD,
)
from tutorial_apm_simple.client.data_classes._cdf_3_d_connection_properties import (
    _CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD,
    _create_cdf_3_d_connection_property_filter,
)

if TYPE_CHECKING:
    from .cdf_3_d_entity_query import CdfEntityQueryAPI


class CdfModelQueryAPI(QueryAPI[T_DomainModelList]):
    def entities(
        self,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> CdfEntityQueryAPI[T_DomainModelList]:
        """Query along the entity edges of the cdf 3 d model.

        Args:
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            min_revision_node_id: The minimum value of the revision node id to filter on.
            max_revision_node_id: The maximum value of the revision node id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            CdfEntityQueryAPI: The query API for the cdf 3 d entity.
        """
        from .cdf_3_d_entity_query import CdfEntityQueryAPI

        edge_view = self._view_by_write_class[CdfConnectionPropertiesApply]
        edge_filter = _create_cdf_3_d_connection_property_filter(
            dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
            edge_view,
            min_revision_id=min_revision_id,
            max_revision_id=max_revision_id,
            min_revision_node_id=min_revision_node_id,
            max_revision_node_id=max_revision_node_id,
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("entities"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(edge_view, list(_CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD.values()))]
                ),
                result_cls=CdfConnectionProperties,
                max_retrieve_limit=limit,
            )
        )
        return CdfEntityQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_cdf_3_d_model: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_cdf_3_d_model: Whether to retrieve the cdf 3 d model or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_cdf_3_d_model and not self._builder[-1].name.startswith("cdf_3_d_model"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("cdf_3_d_model"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[CdfModelApply],
                                list(_CDFMODEL_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=CdfModel,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()