from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite_core.data_classes import (
    DomainModelCore,
    Cognite3DObject,
    Cognite360ImageAnnotation,
)
from cognite_core.data_classes._cognite_360_image import (
    Cognite360Image,
    _create_cognite_360_image_filter,
)
from cognite_core._api._core import (
    DEFAULT_QUERY_LIMIT,
    EdgeQueryStep,
    NodeQueryStep,
    DataClassQueryBuilder,
    QueryAPI,
    T_DomainModelList,
    _create_edge_filter,
)

from cognite_core.data_classes._cognite_360_image_annotation import (
    _create_cognite_360_image_annotation_filter,
)

if TYPE_CHECKING:
    from cognite_core._api.cognite_360_image_query import Cognite360ImageQueryAPI


class Cognite3DObjectQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("cdf_cdm", "Cognite3DObject", "v1")

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
                result_cls=Cognite3DObject,
                max_retrieve_limit=limit,
            )
        )

    def images_360(
        self,
        back: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        bottom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        collection_360: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_euler_rotation_x: float | None = None,
        max_euler_rotation_x: float | None = None,
        min_euler_rotation_y: float | None = None,
        max_euler_rotation_y: float | None = None,
        min_euler_rotation_z: float | None = None,
        max_euler_rotation_z: float | None = None,
        front: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        left: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        right: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_scale_x: float | None = None,
        max_scale_x: float | None = None,
        min_scale_y: float | None = None,
        max_scale_y: float | None = None,
        min_scale_z: float | None = None,
        max_scale_z: float | None = None,
        station_360: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_taken_at: datetime.datetime | None = None,
        max_taken_at: datetime.datetime | None = None,
        top: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_translation_x: float | None = None,
        max_translation_x: float | None = None,
        min_translation_y: float | None = None,
        max_translation_y: float | None = None,
        min_translation_z: float | None = None,
        max_translation_z: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        min_confidence_edge: float | None = None,
        max_confidence_edge: float | None = None,
        description_edge: str | list[str] | None = None,
        description_prefix_edge: str | None = None,
        format_version_edge: str | list[str] | None = None,
        format_version_prefix_edge: str | None = None,
        name_edge: str | list[str] | None = None,
        name_prefix_edge: str | None = None,
        source_edge: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context_edge: str | list[str] | None = None,
        source_context_prefix_edge: str | None = None,
        min_source_created_time_edge: datetime.datetime | None = None,
        max_source_created_time_edge: datetime.datetime | None = None,
        source_created_user_edge: str | list[str] | None = None,
        source_created_user_prefix_edge: str | None = None,
        source_id_edge: str | list[str] | None = None,
        source_id_prefix_edge: str | None = None,
        min_source_updated_time_edge: datetime.datetime | None = None,
        max_source_updated_time_edge: datetime.datetime | None = None,
        source_updated_user_edge: str | list[str] | None = None,
        source_updated_user_prefix_edge: str | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ) -> Cognite360ImageQueryAPI[T_DomainModelList]:
        """Query along the images 360 edges of the Cognite 3D object.

        Args:
            back: The back to filter on.
            bottom: The bottom to filter on.
            collection_360: The collection 360 to filter on.
            min_euler_rotation_x: The minimum value of the euler rotation x to filter on.
            max_euler_rotation_x: The maximum value of the euler rotation x to filter on.
            min_euler_rotation_y: The minimum value of the euler rotation y to filter on.
            max_euler_rotation_y: The maximum value of the euler rotation y to filter on.
            min_euler_rotation_z: The minimum value of the euler rotation z to filter on.
            max_euler_rotation_z: The maximum value of the euler rotation z to filter on.
            front: The front to filter on.
            left: The left to filter on.
            right: The right to filter on.
            min_scale_x: The minimum value of the scale x to filter on.
            max_scale_x: The maximum value of the scale x to filter on.
            min_scale_y: The minimum value of the scale y to filter on.
            max_scale_y: The maximum value of the scale y to filter on.
            min_scale_z: The minimum value of the scale z to filter on.
            max_scale_z: The maximum value of the scale z to filter on.
            station_360: The station 360 to filter on.
            min_taken_at: The minimum value of the taken at to filter on.
            max_taken_at: The maximum value of the taken at to filter on.
            top: The top to filter on.
            min_translation_x: The minimum value of the translation x to filter on.
            max_translation_x: The maximum value of the translation x to filter on.
            min_translation_y: The minimum value of the translation y to filter on.
            max_translation_y: The maximum value of the translation y to filter on.
            min_translation_z: The minimum value of the translation z to filter on.
            max_translation_z: The maximum value of the translation z to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            min_confidence_edge: The minimum value of the confidence to filter on.
            max_confidence_edge: The maximum value of the confidence to filter on.
            description_edge: The description to filter on.
            description_prefix_edge: The prefix of the description to filter on.
            format_version_edge: The format version to filter on.
            format_version_prefix_edge: The prefix of the format version to filter on.
            name_edge: The name to filter on.
            name_prefix_edge: The prefix of the name to filter on.
            source_edge: The source to filter on.
            source_context_edge: The source context to filter on.
            source_context_prefix_edge: The prefix of the source context to filter on.
            min_source_created_time_edge: The minimum value of the source created time to filter on.
            max_source_created_time_edge: The maximum value of the source created time to filter on.
            source_created_user_edge: The source created user to filter on.
            source_created_user_prefix_edge: The prefix of the source created user to filter on.
            source_id_edge: The source id to filter on.
            source_id_prefix_edge: The prefix of the source id to filter on.
            min_source_updated_time_edge: The minimum value of the source updated time to filter on.
            max_source_updated_time_edge: The maximum value of the source updated time to filter on.
            source_updated_user_edge: The source updated user to filter on.
            source_updated_user_prefix_edge: The prefix of the source updated user to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of images 360 edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.

        Returns:
            Cognite360ImageQueryAPI: The query API for the Cognite 360 image.
        """
        from .cognite_360_image_query import Cognite360ImageQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_view = Cognite360ImageAnnotation._view_id
        edge_filter = _create_cognite_360_image_annotation_filter(
            dm.DirectRelationReference("cdf_cdm", "image-360-annotation"),
            edge_view,
            min_confidence=min_confidence_edge,
            max_confidence=max_confidence_edge,
            description=description_edge,
            description_prefix=description_prefix_edge,
            format_version=format_version_edge,
            format_version_prefix=format_version_prefix_edge,
            name=name_edge,
            name_prefix=name_prefix_edge,
            source=source_edge,
            source_context=source_context_edge,
            source_context_prefix=source_context_prefix_edge,
            min_source_created_time=min_source_created_time_edge,
            max_source_created_time=max_source_created_time_edge,
            source_created_user=source_created_user_edge,
            source_created_user_prefix=source_created_user_prefix_edge,
            source_id=source_id_edge,
            source_id_prefix=source_id_prefix_edge,
            min_source_updated_time=min_source_updated_time_edge,
            max_source_updated_time=max_source_updated_time_edge,
            source_updated_user=source_updated_user_edge,
            source_updated_user_prefix=source_updated_user_prefix_edge,
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            EdgeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                result_cls=Cognite360ImageAnnotation,
                max_retrieve_limit=limit,
            )
        )

        view_id = Cognite360ImageQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_cognite_360_image_filter(
            view_id,
            back,
            bottom,
            collection_360,
            min_euler_rotation_x,
            max_euler_rotation_x,
            min_euler_rotation_y,
            max_euler_rotation_y,
            min_euler_rotation_z,
            max_euler_rotation_z,
            front,
            left,
            right,
            min_scale_x,
            max_scale_x,
            min_scale_y,
            max_scale_y,
            min_scale_z,
            max_scale_z,
            station_360,
            min_taken_at,
            max_taken_at,
            top,
            min_translation_x,
            max_translation_x,
            min_translation_y,
            max_translation_y,
            min_translation_z,
            max_translation_z,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return Cognite360ImageQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
