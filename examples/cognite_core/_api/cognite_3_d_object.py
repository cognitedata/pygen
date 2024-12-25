from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core._api.cognite_3_d_object_images_360 import Cognite3DObjectImages360API
from cognite_core._api.cognite_3_d_object_query import Cognite3DObjectQueryAPI
from cognite_core.data_classes import (
    Cognite3DObject,
    Cognite3DObjectFields,
    Cognite3DObjectList,
    Cognite3DObjectTextFields,
    Cognite3DObjectWrite,
    Cognite3DObjectWriteList,
    Cognite360Image,
    Cognite360ImageAnnotation,
    CogniteAsset,
    CogniteCADNode,
    CognitePointCloudVolume,
    ResourcesWriteResult,
)
from cognite_core.data_classes._cognite_3_d_object import (
    _COGNITE3DOBJECT_PROPERTIES_BY_FIELD,
    Cognite3DObjectQuery,
    _create_cognite_3_d_object_filter,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    EdgeQueryStep,
    NodeQueryStep,
)


class Cognite3DObjectAPI(NodeAPI[Cognite3DObject, Cognite3DObjectWrite, Cognite3DObjectList, Cognite3DObjectWriteList]):
    _view_id = dm.ViewId("cdf_cdm", "Cognite3DObject", "v1")
    _properties_by_field = _COGNITE3DOBJECT_PROPERTIES_BY_FIELD
    _class_type = Cognite3DObject
    _class_list = Cognite3DObjectList
    _class_write_list = Cognite3DObjectWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.images_360_edge = Cognite3DObjectImages360API(client)

    def __call__(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> Cognite3DObjectQueryAPI[Cognite3DObjectList]:
        """Query starting at Cognite 3D objects.

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_x_max: The minimum value of the x max to filter on.
            max_x_max: The maximum value of the x max to filter on.
            min_x_min: The minimum value of the x min to filter on.
            max_x_min: The maximum value of the x min to filter on.
            min_y_max: The minimum value of the y max to filter on.
            max_y_max: The maximum value of the y max to filter on.
            min_y_min: The minimum value of the y min to filter on.
            max_y_min: The maximum value of the y min to filter on.
            min_z_max: The minimum value of the z max to filter on.
            max_z_max: The maximum value of the z max to filter on.
            min_z_min: The minimum value of the z min to filter on.
            max_z_min: The maximum value of the z min to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D objects to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite 3D objects.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_3_d_object_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            min_x_max,
            max_x_max,
            min_x_min,
            max_x_min,
            min_y_max,
            max_y_max,
            min_y_min,
            max_y_min,
            min_z_max,
            max_z_max,
            min_z_min,
            max_z_min,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(Cognite3DObjectList)
        return Cognite3DObjectQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        cognite_3_d_object: Cognite3DObjectWrite | Sequence[Cognite3DObjectWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite 3D objects.

        Note: This method iterates through all nodes and timeseries linked to cognite_3_d_object and creates them including the edges
        between the nodes. For example, if any of `images_360` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            cognite_3_d_object: Cognite 3d object or sequence of Cognite 3D objects to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cognite_3_d_object:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import Cognite3DObjectWrite
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_object = Cognite3DObjectWrite(external_id="my_cognite_3_d_object", ...)
                >>> result = client.cognite_3_d_object.apply(cognite_3_d_object)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_3_d_object.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_3_d_object, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite 3D object.

        Args:
            external_id: External id of the Cognite 3D object to delete.
            space: The space where all the Cognite 3D object are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_3_d_object by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_3_d_object.delete("my_cognite_3_d_object")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_3_d_object.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Cognite3DObject | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Cognite3DObjectList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> Cognite3DObject | Cognite3DObjectList | None:
        """Retrieve one or more Cognite 3D objects by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite 3D objects.
            space: The space where all the Cognite 3D objects are located.

        Returns:
            The requested Cognite 3D objects.

        Examples:

            Retrieve cognite_3_d_object by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_object = client.cognite_3_d_object.retrieve("my_cognite_3_d_object")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.images_360_edge,
                    "images_360",
                    dm.DirectRelationReference("cdf_cdm", "image-360-annotation"),
                    "outwards",
                    dm.ViewId("cdf_cdm", "Cognite360Image", "v1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Cognite3DObjectList:
        """Search Cognite 3D objects

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_x_max: The minimum value of the x max to filter on.
            max_x_max: The maximum value of the x max to filter on.
            min_x_min: The minimum value of the x min to filter on.
            max_x_min: The maximum value of the x min to filter on.
            min_y_max: The minimum value of the y max to filter on.
            max_y_max: The maximum value of the y max to filter on.
            min_y_min: The minimum value of the y min to filter on.
            max_y_min: The maximum value of the y min to filter on.
            min_z_max: The minimum value of the z max to filter on.
            max_z_max: The maximum value of the z max to filter on.
            min_z_min: The minimum value of the z min to filter on.
            max_z_min: The maximum value of the z min to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D objects to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite 3D objects matching the query.

        Examples:

           Search for 'my_cognite_3_d_object' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_objects = client.cognite_3_d_object.search('my_cognite_3_d_object')

        """
        filter_ = _create_cognite_3_d_object_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            min_x_max,
            max_x_max,
            min_x_min,
            max_x_min,
            min_y_max,
            max_y_max,
            min_y_min,
            max_y_min,
            min_z_max,
            max_z_max,
            min_z_min,
            max_z_min,
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
        property: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        query: str | None = None,
        search_property: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
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
        property: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        query: str | None = None,
        search_property: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
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
        group_by: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields],
        property: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        query: str | None = None,
        search_property: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
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
        group_by: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        property: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        query: str | None = None,
        search_property: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite 3D objects

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_x_max: The minimum value of the x max to filter on.
            max_x_max: The maximum value of the x max to filter on.
            min_x_min: The minimum value of the x min to filter on.
            max_x_min: The maximum value of the x min to filter on.
            min_y_max: The minimum value of the y max to filter on.
            max_y_max: The maximum value of the y max to filter on.
            min_y_min: The minimum value of the y min to filter on.
            max_y_min: The maximum value of the y min to filter on.
            min_z_max: The minimum value of the z max to filter on.
            max_z_max: The maximum value of the z max to filter on.
            min_z_min: The minimum value of the z min to filter on.
            max_z_min: The maximum value of the z min to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D objects to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite 3D objects in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_3_d_object.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_3_d_object_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            min_x_max,
            max_x_max,
            min_x_min,
            max_x_min,
            min_y_max,
            max_y_max,
            min_y_min,
            max_y_min,
            min_z_max,
            max_z_max,
            min_z_min,
            max_z_min,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: Cognite3DObjectFields,
        interval: float,
        query: str | None = None,
        search_property: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite 3D objects

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_x_max: The minimum value of the x max to filter on.
            max_x_max: The maximum value of the x max to filter on.
            min_x_min: The minimum value of the x min to filter on.
            max_x_min: The maximum value of the x min to filter on.
            min_y_max: The minimum value of the y max to filter on.
            max_y_max: The maximum value of the y max to filter on.
            min_y_min: The minimum value of the y min to filter on.
            max_y_min: The maximum value of the y min to filter on.
            min_z_max: The minimum value of the z max to filter on.
            max_z_max: The maximum value of the z max to filter on.
            min_z_min: The minimum value of the z min to filter on.
            max_z_min: The maximum value of the z min to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D objects to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_3_d_object_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            min_x_max,
            max_x_max,
            min_x_min,
            max_x_min,
            min_y_max,
            max_y_max,
            min_y_min,
            max_y_min,
            min_z_max,
            max_z_max,
            min_z_min,
            max_z_min,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def query(self) -> Cognite3DObjectQuery:
        """Start a query for Cognite 3D objects."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return Cognite3DObjectQuery(self._client)

    def select(self) -> Cognite3DObjectQuery:
        """Start selecting from Cognite 3D objects."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return Cognite3DObjectQuery(self._client)

    def list(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite3DObjectFields | Sequence[Cognite3DObjectFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite3DObjectList:
        """List/filter Cognite 3D objects

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_x_max: The minimum value of the x max to filter on.
            max_x_max: The maximum value of the x max to filter on.
            min_x_min: The minimum value of the x min to filter on.
            max_x_min: The maximum value of the x min to filter on.
            min_y_max: The minimum value of the y max to filter on.
            max_y_max: The maximum value of the y max to filter on.
            min_y_min: The minimum value of the y min to filter on.
            max_y_min: The maximum value of the y min to filter on.
            min_z_max: The minimum value of the z max to filter on.
            max_z_max: The maximum value of the z max to filter on.
            min_z_min: The minimum value of the z min to filter on.
            max_z_min: The maximum value of the z min to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D objects to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `asset`, `cad_nodes`, `images_360` and `point_cloud_volumes` for the Cognite 3D objects. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested Cognite 3D objects

        Examples:

            List Cognite 3D objects and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_objects = client.cognite_3_d_object.list(limit=5)

        """
        filter_ = _create_cognite_3_d_object_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            min_x_max,
            max_x_max,
            min_x_min,
            max_x_min,
            min_y_max,
            max_y_max,
            min_y_min,
            max_y_min,
            min_z_max,
            max_z_max,
            min_z_min,
            max_z_min,
            external_id_prefix,
            space,
            filter,
        )

        if retrieve_connections == "skip":
            return self._list(
                limit=limit,
                filter=filter_,
                sort_by=sort_by,  # type: ignore[arg-type]
                direction=direction,
                sort=sort,
            )

        builder = DataClassQueryBuilder(Cognite3DObjectList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                Cognite3DObject,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        edge_images_360 = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_images_360,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
                Cognite360ImageAnnotation,
            )
        )
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name(edge_images_360),
                    dm.query.NodeResultSetExpression(
                        from_=edge_images_360,
                        filter=dm.filters.HasData(views=[Cognite360Image._view_id]),
                    ),
                    Cognite360Image,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteAsset._view_id]),
                        direction="inwards",
                        through=CogniteAsset._view_id.as_property_ref("object3D"),
                    ),
                    CogniteAsset,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteCADNode._view_id]),
                        direction="inwards",
                        through=CogniteCADNode._view_id.as_property_ref("object3D"),
                    ),
                    CogniteCADNode,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CognitePointCloudVolume._view_id]),
                        direction="inwards",
                        through=CognitePointCloudVolume._view_id.as_property_ref("object3D"),
                    ),
                    CognitePointCloudVolume,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
