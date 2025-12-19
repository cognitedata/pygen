from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Literal
from cognite.client import data_modeling as dm

from cognite_core.data_classes import (
    Cognite360ImageAnnotation,
    Cognite360ImageAnnotationList,
    Cognite360ImageAnnotationWrite,
)
from cognite_core.data_classes._cognite_360_image_annotation import _create_cognite_360_image_annotation_filter

from cognite_core._api._core import DEFAULT_LIMIT_READ, EdgePropertyAPI
from cognite_core.data_classes._core import DEFAULT_INSTANCE_SPACE


class Cognite3DObjectImages360API(EdgePropertyAPI):
    _view_id = dm.ViewId("cdf_cdm", "Cognite360ImageAnnotation", "v1")
    _class_type = Cognite360ImageAnnotation
    _class_write_type = Cognite360ImageAnnotationWrite
    _class_list = Cognite360ImageAnnotationList

    def list(
        self,
        from_cognite_3_d_object: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_cognite_3_d_object_space: str = DEFAULT_INSTANCE_SPACE,
        to_cognite_360_image: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_cognite_360_image_space: str = DEFAULT_INSTANCE_SPACE,
        min_confidence: float | None = None,
        max_confidence: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        format_version: str | list[str] | None = None,
        format_version_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        status: (
            Literal["Approved", "Rejected", "Suggested"] | list[Literal["Approved", "Rejected", "Suggested"]] | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> Cognite360ImageAnnotationList:
        """List images 360 edges of a Cognite 3D object.

        Args:
            from_cognite_3_d_object: ID of the source Cognite 3D object.
            from_cognite_3_d_object_space: Location of the Cognite 3D objects.
            to_cognite_360_image: ID of the target Cognite 360 image.
            to_cognite_360_image_space: Location of the Cognite 360 images.
            min_confidence: The minimum value of the confidence to filter on.
            max_confidence: The maximum value of the confidence to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            format_version: The format version to filter on.
            format_version_prefix: The prefix of the format version to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            status: The status to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of images 360 edges to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.

        Returns:
            The requested images 360 edges.

        Examples:

            List 5 images 360 edges connected to "my_cognite_3_d_object":

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_object = client.cognite_3_d_object.images_360_edge.list(
                ...     "my_cognite_3_d_object", limit=5
                ... )

        """
        filter_ = _create_cognite_360_image_annotation_filter(
            dm.DirectRelationReference("cdf_cdm", "image-360-annotation"),
            self._view_id,
            from_cognite_3_d_object,
            from_cognite_3_d_object_space,
            to_cognite_360_image,
            to_cognite_360_image_space,
            min_confidence,
            max_confidence,
            description,
            description_prefix,
            format_version,
            format_version_prefix,
            name,
            name_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            status,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
