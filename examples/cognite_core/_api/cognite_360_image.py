from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core.config import global_config
from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite_core.data_classes._cognite_360_image import (
    Cognite360ImageQuery,
    _COGNITE360IMAGE_PROPERTIES_BY_FIELD,
    _create_cognite_360_image_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Cognite360Image,
    Cognite360ImageWrite,
    Cognite360ImageFields,
    Cognite360ImageList,
    Cognite360ImageWriteList,
    Cognite360ImageTextFields,
    Cognite360ImageCollection,
    Cognite360ImageStation,
    CogniteFile,
)
from cognite_core._api.cognite_360_image_query import Cognite360ImageQueryAPI


class Cognite360ImageAPI(NodeAPI[Cognite360Image, Cognite360ImageWrite, Cognite360ImageList, Cognite360ImageWriteList]):
    _view_id = dm.ViewId("cdf_cdm", "Cognite360Image", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITE360IMAGE_PROPERTIES_BY_FIELD
    _class_type = Cognite360Image
    _class_list = Cognite360ImageList
    _class_write_list = Cognite360ImageWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
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
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> Cognite360ImageQueryAPI[Cognite360Image, Cognite360ImageList]:
        """Query starting at Cognite 360 images.

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
            limit: Maximum number of Cognite 360 images to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite 360 images.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_360_image_filter(
            self._view_id,
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
        return Cognite360ImageQueryAPI(
            self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit
        )

    def apply(
        self,
        cognite_360_image: Cognite360ImageWrite | Sequence[Cognite360ImageWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite 360 images.

        Args:
            cognite_360_image: Cognite 360 image or
                sequence of Cognite 360 images to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None.
                However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cognite_360_image:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import Cognite360ImageWrite
                >>> client = CogniteCoreClient()
                >>> cognite_360_image = Cognite360ImageWrite(
                ...     external_id="my_cognite_360_image", ...
                ... )
                >>> result = client.cognite_360_image.apply(cognite_360_image)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_360_image.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_360_image, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite 360 image.

        Args:
            external_id: External id of the Cognite 360 image to delete.
            space: The space where all the Cognite 360 image are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_360_image by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_360_image.delete("my_cognite_360_image")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_360_image.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite360Image | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite360ImageList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite360Image | Cognite360ImageList | None:
        """Retrieve one or more Cognite 360 images by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite 360 images.
            space: The space where all the Cognite 360 images are located.
            retrieve_connections: Whether to retrieve `back`, `bottom`, `collection_360`, `front`, `left`, `right`,
            `station_360` and `top` for the Cognite 360 images. Defaults to 'skip'.'skip' will not retrieve any
            connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve
            the full connected items.

        Returns:
            The requested Cognite 360 images.

        Examples:

            Retrieve cognite_360_image by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_360_image = client.cognite_360_image.retrieve(
                ...     "my_cognite_360_image"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

    def search(
        self,
        query: str,
        properties: Cognite360ImageTextFields | SequenceNotStr[Cognite360ImageTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite360ImageFields | SequenceNotStr[Cognite360ImageFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Cognite360ImageList:
        """Search Cognite 360 images

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
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
            limit: Maximum number of Cognite 360 images to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite 360 images matching the query.

        Examples:

           Search for 'my_cognite_360_image' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_360_images = client.cognite_360_image.search(
                ...     'my_cognite_360_image'
                ... )

        """
        filter_ = _create_cognite_360_image_filter(
            self._view_id,
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
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: Cognite360ImageFields | SequenceNotStr[Cognite360ImageFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: Cognite360ImageFields | SequenceNotStr[Cognite360ImageFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: Cognite360ImageFields | SequenceNotStr[Cognite360ImageFields],
        property: Cognite360ImageFields | SequenceNotStr[Cognite360ImageFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: Cognite360ImageFields | SequenceNotStr[Cognite360ImageFields] | None = None,
        property: Cognite360ImageFields | SequenceNotStr[Cognite360ImageFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite 360 images

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
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
            limit: Maximum number of Cognite 360 images to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite 360 images in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_360_image.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_360_image_filter(
            self._view_id,
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
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=None,
            search_properties=None,
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: Cognite360ImageFields,
        interval: float,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite 360 images

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
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
            limit: Maximum number of Cognite 360 images to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_360_image_filter(
            self._view_id,
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
            filter,
        )
        return self._histogram(
            property,
            interval,
            None,
            None,
            limit,
            filter_,
        )

    def query(self) -> Cognite360ImageQuery:
        """Start a query for Cognite 360 images."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return Cognite360ImageQuery(self._client)

    def select(self) -> Cognite360ImageQuery:
        """Start selecting from Cognite 360 images."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return Cognite360ImageQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> Cognite360ImageList:
        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=sort,
                limit=limit,
                has_container_fields=True,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    CogniteFile._view_id,
                    ViewPropertyId(self._view_id, "back"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    CogniteFile._view_id,
                    ViewPropertyId(self._view_id, "bottom"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    Cognite360ImageCollection._view_id,
                    ViewPropertyId(self._view_id, "collection360"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    CogniteFile._view_id,
                    ViewPropertyId(self._view_id, "front"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    CogniteFile._view_id,
                    ViewPropertyId(self._view_id, "left"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    CogniteFile._view_id,
                    ViewPropertyId(self._view_id, "right"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    Cognite360ImageStation._view_id,
                    ViewPropertyId(self._view_id, "station360"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    CogniteFile._view_id,
                    ViewPropertyId(self._view_id, "top"),
                    has_container_fields=True,
                )
            )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        unpacked = QueryUnpacker(builder, edges=unpack_edges).unpack()
        if global_config.validate_retrieve:
            retrieved = [Cognite360Image.model_validate(item) for item in unpacked]
        else:
            retrieved = [Cognite360Image.model_construct(**item) for item in unpacked]
        return Cognite360ImageList(retrieved)

    def list(
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite360ImageFields | Sequence[Cognite360ImageFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite360ImageList:
        """List/filter Cognite 360 images

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
            limit: Maximum number of Cognite 360 images to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `back`, `bottom`, `collection_360`, `front`, `left`, `right`,
            `station_360` and `top` for the Cognite 360 images. Defaults to 'skip'.'skip' will not retrieve any
            connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve
            the full connected items.

        Returns:
            List of requested Cognite 360 images

        Examples:

            List Cognite 360 images and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_360_images = client.cognite_360_image.list(limit=5)

        """
        filter_ = _create_cognite_360_image_filter(
            self._view_id,
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
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input)
