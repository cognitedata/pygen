from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite_core._api._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    QueryAPI,
    T_DomainModelList,
)
from cognite_core.data_classes import (
    Cognite3DObject,
    CogniteCADModel,
    CognitePointCloudVolume,
)


class CognitePointCloudVolumeQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("cdf_cdm", "CognitePointCloudVolume", "v1")

    def __init__(
        self,
        client: CogniteClient,
        builder: DataClassQueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)
        from_ = self._builder.get_from()
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                result_cls=CognitePointCloudVolume,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_model_3d: bool = False,
        retrieve_object_3d: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_model_3d: Whether to retrieve the model 3d for each Cognite point cloud volume or not.
            retrieve_object_3d: Whether to retrieve the object 3d for each Cognite point cloud volume or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_model_3d:
            self._query_append_model_3d(from_)
        if retrieve_object_3d:
            self._query_append_object_3d(from_)
        return self._query()

    def _query_append_model_3d(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("model3D"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteCADModel._view_id]),
                ),
                result_cls=CogniteCADModel,
            ),
        )

    def _query_append_object_3d(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("object3D"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Cognite3DObject._view_id]),
                ),
                result_cls=Cognite3DObject,
            ),
        )
