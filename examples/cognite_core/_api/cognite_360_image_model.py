from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core._api.cognite_360_image_model_query import Cognite360ImageModelQueryAPI
from cognite_core.data_classes import (
    Cognite360ImageCollection,
    Cognite360ImageModel,
    Cognite360ImageModelFields,
    Cognite360ImageModelList,
    Cognite360ImageModelTextFields,
    Cognite360ImageModelWrite,
    Cognite360ImageModelWriteList,
    CogniteFile,
    ResourcesWriteResult,
)
from cognite_core.data_classes._cognite_360_image_model import (
    _COGNITE360IMAGEMODEL_PROPERTIES_BY_FIELD,
    Cognite360ImageModelQuery,
    _create_cognite_360_image_model_filter,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
)


class Cognite360ImageModelAPI(
    NodeAPI[Cognite360ImageModel, Cognite360ImageModelWrite, Cognite360ImageModelList, Cognite360ImageModelWriteList]
):
    _view_id = dm.ViewId("cdf_cdm", "Cognite360ImageModel", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITE360IMAGEMODEL_PROPERTIES_BY_FIELD
    _class_type = Cognite360ImageModel
    _class_list = Cognite360ImageModelList
    _class_write_list = Cognite360ImageModelWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        thumbnail: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> Cognite360ImageModelQueryAPI[Cognite360ImageModelList]:
        """Query starting at Cognite 360 image models.

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            thumbnail: The thumbnail to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 360 image models to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite 360 image models.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_360_image_model_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            thumbnail,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(Cognite360ImageModelList)
        return Cognite360ImageModelQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        cognite_360_image_model: Cognite360ImageModelWrite | Sequence[Cognite360ImageModelWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite 360 image models.

        Args:
            cognite_360_image_model: Cognite 360 image model or
                sequence of Cognite 360 image models to upsert.
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

            Create a new cognite_360_image_model:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import Cognite360ImageModelWrite
                >>> client = CogniteCoreClient()
                >>> cognite_360_image_model = Cognite360ImageModelWrite(
                ...     external_id="my_cognite_360_image_model", ...
                ... )
                >>> result = client.cognite_360_image_model.apply(cognite_360_image_model)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_360_image_model.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_360_image_model, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite 360 image model.

        Args:
            external_id: External id of the Cognite 360 image model to delete.
            space: The space where all the Cognite 360 image model are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_360_image_model by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_360_image_model.delete("my_cognite_360_image_model")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_360_image_model.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Cognite360ImageModel | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Cognite360ImageModelList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> Cognite360ImageModel | Cognite360ImageModelList | None:
        """Retrieve one or more Cognite 360 image models by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite 360 image models.
            space: The space where all the Cognite 360 image models are located.

        Returns:
            The requested Cognite 360 image models.

        Examples:

            Retrieve cognite_360_image_model by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_360_image_model = client.cognite_360_image_model.retrieve(
                ...     "my_cognite_360_image_model"
                ... )

        """
        return self._retrieve(external_id, space, retrieve_edges=True, edge_api_name_type_direction_view_id_penta=[])

    def search(
        self,
        query: str,
        properties: Cognite360ImageModelTextFields | SequenceNotStr[Cognite360ImageModelTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        thumbnail: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite360ImageModelFields | SequenceNotStr[Cognite360ImageModelFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Cognite360ImageModelList:
        """Search Cognite 360 image models

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            thumbnail: The thumbnail to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 360 image models to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite 360 image models matching the query.

        Examples:

           Search for 'my_cognite_360_image_model' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_360_image_models = client.cognite_360_image_model.search(
                ...     'my_cognite_360_image_model'
                ... )

        """
        filter_ = _create_cognite_360_image_model_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            thumbnail,
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
        property: Cognite360ImageModelFields | SequenceNotStr[Cognite360ImageModelFields] | None = None,
        query: str | None = None,
        search_property: Cognite360ImageModelTextFields | SequenceNotStr[Cognite360ImageModelTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        thumbnail: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        property: Cognite360ImageModelFields | SequenceNotStr[Cognite360ImageModelFields] | None = None,
        query: str | None = None,
        search_property: Cognite360ImageModelTextFields | SequenceNotStr[Cognite360ImageModelTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        thumbnail: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: Cognite360ImageModelFields | SequenceNotStr[Cognite360ImageModelFields],
        property: Cognite360ImageModelFields | SequenceNotStr[Cognite360ImageModelFields] | None = None,
        query: str | None = None,
        search_property: Cognite360ImageModelTextFields | SequenceNotStr[Cognite360ImageModelTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        thumbnail: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: Cognite360ImageModelFields | SequenceNotStr[Cognite360ImageModelFields] | None = None,
        property: Cognite360ImageModelFields | SequenceNotStr[Cognite360ImageModelFields] | None = None,
        query: str | None = None,
        search_property: Cognite360ImageModelTextFields | SequenceNotStr[Cognite360ImageModelTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        thumbnail: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite 360 image models

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
            thumbnail: The thumbnail to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 360 image models to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite 360 image models in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_360_image_model.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_360_image_model_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            thumbnail,
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
        property: Cognite360ImageModelFields,
        interval: float,
        query: str | None = None,
        search_property: Cognite360ImageModelTextFields | SequenceNotStr[Cognite360ImageModelTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        thumbnail: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite 360 image models

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            thumbnail: The thumbnail to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 360 image models to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_360_image_model_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            thumbnail,
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

    def query(self) -> Cognite360ImageModelQuery:
        """Start a query for Cognite 360 image models."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return Cognite360ImageModelQuery(self._client)

    def select(self) -> Cognite360ImageModelQuery:
        """Start selecting from Cognite 360 image models."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return Cognite360ImageModelQuery(self._client)

    def list(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        thumbnail: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite360ImageModelFields | Sequence[Cognite360ImageModelFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite360ImageModelList:
        """List/filter Cognite 360 image models

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            thumbnail: The thumbnail to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 360 image models to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `collections` and `thumbnail` for the Cognite 360 image models.
            Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier
            of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested Cognite 360 image models

        Examples:

            List Cognite 360 image models and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_360_image_models = client.cognite_360_image_model.list(limit=5)

        """
        filter_ = _create_cognite_360_image_model_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            thumbnail,
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

        builder = DataClassQueryBuilder(Cognite360ImageModelList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                Cognite360ImageModel,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[Cognite360ImageCollection._view_id]),
                        direction="inwards",
                        through=Cognite360ImageCollection._view_id.as_property_ref("model3D"),
                    ),
                    Cognite360ImageCollection,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("thumbnail"),
                    ),
                    CogniteFile,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
