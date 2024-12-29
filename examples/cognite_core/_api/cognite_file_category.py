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
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite_core.data_classes._cognite_file_category import (
    CogniteFileCategoryQuery,
    _COGNITEFILECATEGORY_PROPERTIES_BY_FIELD,
    _create_cognite_file_category_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CogniteFileCategory,
    CogniteFileCategoryWrite,
    CogniteFileCategoryFields,
    CogniteFileCategoryList,
    CogniteFileCategoryWriteList,
    CogniteFileCategoryTextFields,
)
from cognite_core._api.cognite_file_category_query import CogniteFileCategoryQueryAPI


class CogniteFileCategoryAPI(
    NodeAPI[CogniteFileCategory, CogniteFileCategoryWrite, CogniteFileCategoryList, CogniteFileCategoryWriteList]
):
    _view_id = dm.ViewId("cdf_cdm", "CogniteFileCategory", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITEFILECATEGORY_PROPERTIES_BY_FIELD
    _class_type = CogniteFileCategory
    _class_list = CogniteFileCategoryList
    _class_write_list = CogniteFileCategoryWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogniteFileCategoryQueryAPI[CogniteFileCategory, CogniteFileCategoryList]:
        """Query starting at Cognite file categories.

        Args:
            code: The code to filter on.
            code_prefix: The prefix of the code to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            standard: The standard to filter on.
            standard_prefix: The prefix of the standard to filter on.
            standard_reference: The standard reference to filter on.
            standard_reference_prefix: The prefix of the standard reference to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite file categories to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite file categories.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_file_category_filter(
            self._view_id,
            code,
            code_prefix,
            description,
            description_prefix,
            name,
            name_prefix,
            standard,
            standard_prefix,
            standard_reference,
            standard_reference_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return CogniteFileCategoryQueryAPI(
            self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit
        )

    def apply(
        self,
        cognite_file_category: CogniteFileCategoryWrite | Sequence[CogniteFileCategoryWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite file categories.

        Args:
            cognite_file_category: Cognite file category or
                sequence of Cognite file categories to upsert.
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

            Create a new cognite_file_category:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CogniteFileCategoryWrite
                >>> client = CogniteCoreClient()
                >>> cognite_file_category = CogniteFileCategoryWrite(
                ...     external_id="my_cognite_file_category", ...
                ... )
                >>> result = client.cognite_file_category.apply(cognite_file_category)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_file_category.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_file_category, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite file category.

        Args:
            external_id: External id of the Cognite file category to delete.
            space: The space where all the Cognite file category are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_file_category by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_file_category.delete("my_cognite_file_category")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_file_category.delete(my_ids)` please use `my_client.delete(my_ids)`."
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
    ) -> CogniteFileCategory | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteFileCategoryList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteFileCategory | CogniteFileCategoryList | None:
        """Retrieve one or more Cognite file categories by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite file categories.
            space: The space where all the Cognite file categories are located.

        Returns:
            The requested Cognite file categories.

        Examples:

            Retrieve cognite_file_category by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_file_category = client.cognite_file_category.retrieve(
                ...     "my_cognite_file_category"
                ... )

        """
        return self._retrieve(external_id, space, retrieve_connections)

    def search(
        self,
        query: str,
        properties: CogniteFileCategoryTextFields | SequenceNotStr[CogniteFileCategoryTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteFileCategoryFields | SequenceNotStr[CogniteFileCategoryFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteFileCategoryList:
        """Search Cognite file categories

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            code: The code to filter on.
            code_prefix: The prefix of the code to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            standard: The standard to filter on.
            standard_prefix: The prefix of the standard to filter on.
            standard_reference: The standard reference to filter on.
            standard_reference_prefix: The prefix of the standard reference to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite file categories to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite file categories matching the query.

        Examples:

           Search for 'my_cognite_file_category' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_file_categories = client.cognite_file_category.search(
                ...     'my_cognite_file_category'
                ... )

        """
        filter_ = _create_cognite_file_category_filter(
            self._view_id,
            code,
            code_prefix,
            description,
            description_prefix,
            name,
            name_prefix,
            standard,
            standard_prefix,
            standard_reference,
            standard_reference_prefix,
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
        property: CogniteFileCategoryFields | SequenceNotStr[CogniteFileCategoryFields] | None = None,
        query: str | None = None,
        search_property: CogniteFileCategoryTextFields | SequenceNotStr[CogniteFileCategoryTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
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
        property: CogniteFileCategoryFields | SequenceNotStr[CogniteFileCategoryFields] | None = None,
        query: str | None = None,
        search_property: CogniteFileCategoryTextFields | SequenceNotStr[CogniteFileCategoryTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
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
        group_by: CogniteFileCategoryFields | SequenceNotStr[CogniteFileCategoryFields],
        property: CogniteFileCategoryFields | SequenceNotStr[CogniteFileCategoryFields] | None = None,
        query: str | None = None,
        search_property: CogniteFileCategoryTextFields | SequenceNotStr[CogniteFileCategoryTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
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
        group_by: CogniteFileCategoryFields | SequenceNotStr[CogniteFileCategoryFields] | None = None,
        property: CogniteFileCategoryFields | SequenceNotStr[CogniteFileCategoryFields] | None = None,
        query: str | None = None,
        search_property: CogniteFileCategoryTextFields | SequenceNotStr[CogniteFileCategoryTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite file categories

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            code: The code to filter on.
            code_prefix: The prefix of the code to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            standard: The standard to filter on.
            standard_prefix: The prefix of the standard to filter on.
            standard_reference: The standard reference to filter on.
            standard_reference_prefix: The prefix of the standard reference to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite file categories to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite file categories in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_file_category.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_file_category_filter(
            self._view_id,
            code,
            code_prefix,
            description,
            description_prefix,
            name,
            name_prefix,
            standard,
            standard_prefix,
            standard_reference,
            standard_reference_prefix,
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
        property: CogniteFileCategoryFields,
        interval: float,
        query: str | None = None,
        search_property: CogniteFileCategoryTextFields | SequenceNotStr[CogniteFileCategoryTextFields] | None = None,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite file categories

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            code: The code to filter on.
            code_prefix: The prefix of the code to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            standard: The standard to filter on.
            standard_prefix: The prefix of the standard to filter on.
            standard_reference: The standard reference to filter on.
            standard_reference_prefix: The prefix of the standard reference to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite file categories to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_file_category_filter(
            self._view_id,
            code,
            code_prefix,
            description,
            description_prefix,
            name,
            name_prefix,
            standard,
            standard_prefix,
            standard_reference,
            standard_reference_prefix,
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

    def query(self) -> CogniteFileCategoryQuery:
        """Start a query for Cognite file categories."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return CogniteFileCategoryQuery(self._client)

    def select(self) -> CogniteFileCategoryQuery:
        """Start selecting from Cognite file categories."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return CogniteFileCategoryQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> CogniteFileCategoryList:
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
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        unpacked = QueryUnpacker(builder, edges=unpack_edges).unpack()
        return CogniteFileCategoryList([CogniteFileCategory.model_validate(item) for item in unpacked])

    def list(
        self,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        standard: str | list[str] | None = None,
        standard_prefix: str | None = None,
        standard_reference: str | list[str] | None = None,
        standard_reference_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteFileCategoryFields | Sequence[CogniteFileCategoryFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteFileCategoryList:
        """List/filter Cognite file categories

        Args:
            code: The code to filter on.
            code_prefix: The prefix of the code to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            standard: The standard to filter on.
            standard_prefix: The prefix of the standard to filter on.
            standard_reference: The standard reference to filter on.
            standard_reference_prefix: The prefix of the standard reference to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite file categories to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested Cognite file categories

        Examples:

            List Cognite file categories and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_file_categories = client.cognite_file_category.list(limit=5)

        """
        filter_ = _create_cognite_file_category_filter(
            self._view_id,
            code,
            code_prefix,
            description,
            description_prefix,
            name,
            name_prefix,
            standard,
            standard_prefix,
            standard_reference,
            standard_reference_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
