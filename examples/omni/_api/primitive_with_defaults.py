from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from omni.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PrimitiveWithDefaults,
    PrimitiveWithDefaultsWrite,
    PrimitiveWithDefaultsFields,
    PrimitiveWithDefaultsList,
    PrimitiveWithDefaultsWriteList,
    PrimitiveWithDefaultsTextFields,
)
from omni.data_classes._primitive_with_defaults import (
    PrimitiveWithDefaultsQuery,
    _PRIMITIVEWITHDEFAULTS_PROPERTIES_BY_FIELD,
    _create_primitive_with_default_filter,
)
from omni._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from omni._api.primitive_with_defaults_query import PrimitiveWithDefaultsQueryAPI


class PrimitiveWithDefaultsAPI(
    NodeAPI[
        PrimitiveWithDefaults, PrimitiveWithDefaultsWrite, PrimitiveWithDefaultsList, PrimitiveWithDefaultsWriteList
    ]
):
    _view_id = dm.ViewId("sp_pygen_models", "PrimitiveWithDefaults", "1")
    _properties_by_field = _PRIMITIVEWITHDEFAULTS_PROPERTIES_BY_FIELD
    _class_type = PrimitiveWithDefaults
    _class_list = PrimitiveWithDefaultsList
    _class_write_list = PrimitiveWithDefaultsWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        min_auto_increment_int_32: int | None = None,
        max_auto_increment_int_32: int | None = None,
        default_boolean: bool | None = None,
        min_default_float_32: float | None = None,
        max_default_float_32: float | None = None,
        default_string: str | list[str] | None = None,
        default_string_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PrimitiveWithDefaultsQueryAPI[PrimitiveWithDefaultsList]:
        """Query starting at primitive with defaults.

        Args:
            min_auto_increment_int_32: The minimum value of the auto increment int 32 to filter on.
            max_auto_increment_int_32: The maximum value of the auto increment int 32 to filter on.
            default_boolean: The default boolean to filter on.
            min_default_float_32: The minimum value of the default float 32 to filter on.
            max_default_float_32: The maximum value of the default float 32 to filter on.
            default_string: The default string to filter on.
            default_string_prefix: The prefix of the default string to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive with defaults to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for primitive with defaults.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_primitive_with_default_filter(
            self._view_id,
            min_auto_increment_int_32,
            max_auto_increment_int_32,
            default_boolean,
            min_default_float_32,
            max_default_float_32,
            default_string,
            default_string_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(PrimitiveWithDefaultsList)
        return PrimitiveWithDefaultsQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        primitive_with_default: PrimitiveWithDefaultsWrite | Sequence[PrimitiveWithDefaultsWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) primitive with defaults.

        Args:
            primitive_with_default: Primitive with default or sequence of primitive with defaults to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new primitive_with_default:

                >>> from omni import OmniClient
                >>> from omni.data_classes import PrimitiveWithDefaultsWrite
                >>> client = OmniClient()
                >>> primitive_with_default = PrimitiveWithDefaultsWrite(external_id="my_primitive_with_default", ...)
                >>> result = client.primitive_with_defaults.apply(primitive_with_default)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.primitive_with_defaults.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(primitive_with_default, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more primitive with default.

        Args:
            external_id: External id of the primitive with default to delete.
            space: The space where all the primitive with default are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete primitive_with_default by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.primitive_with_defaults.delete("my_primitive_with_default")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.primitive_with_defaults.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveWithDefaults | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveWithDefaultsList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> PrimitiveWithDefaults | PrimitiveWithDefaultsList | None:
        """Retrieve one or more primitive with defaults by id(s).

        Args:
            external_id: External id or list of external ids of the primitive with defaults.
            space: The space where all the primitive with defaults are located.

        Returns:
            The requested primitive with defaults.

        Examples:

            Retrieve primitive_with_default by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_with_default = client.primitive_with_defaults.retrieve("my_primitive_with_default")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PrimitiveWithDefaultsTextFields | SequenceNotStr[PrimitiveWithDefaultsTextFields] | None = None,
        min_auto_increment_int_32: int | None = None,
        max_auto_increment_int_32: int | None = None,
        default_boolean: bool | None = None,
        min_default_float_32: float | None = None,
        max_default_float_32: float | None = None,
        default_string: str | list[str] | None = None,
        default_string_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PrimitiveWithDefaultsFields | SequenceNotStr[PrimitiveWithDefaultsFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PrimitiveWithDefaultsList:
        """Search primitive with defaults

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_auto_increment_int_32: The minimum value of the auto increment int 32 to filter on.
            max_auto_increment_int_32: The maximum value of the auto increment int 32 to filter on.
            default_boolean: The default boolean to filter on.
            min_default_float_32: The minimum value of the default float 32 to filter on.
            max_default_float_32: The maximum value of the default float 32 to filter on.
            default_string: The default string to filter on.
            default_string_prefix: The prefix of the default string to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive with defaults to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results primitive with defaults matching the query.

        Examples:

           Search for 'my_primitive_with_default' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_with_defaults = client.primitive_with_defaults.search('my_primitive_with_default')

        """
        filter_ = _create_primitive_with_default_filter(
            self._view_id,
            min_auto_increment_int_32,
            max_auto_increment_int_32,
            default_boolean,
            min_default_float_32,
            max_default_float_32,
            default_string,
            default_string_prefix,
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
        property: PrimitiveWithDefaultsFields | SequenceNotStr[PrimitiveWithDefaultsFields] | None = None,
        query: str | None = None,
        search_property: (
            PrimitiveWithDefaultsTextFields | SequenceNotStr[PrimitiveWithDefaultsTextFields] | None
        ) = None,
        min_auto_increment_int_32: int | None = None,
        max_auto_increment_int_32: int | None = None,
        default_boolean: bool | None = None,
        min_default_float_32: float | None = None,
        max_default_float_32: float | None = None,
        default_string: str | list[str] | None = None,
        default_string_prefix: str | None = None,
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
        property: PrimitiveWithDefaultsFields | SequenceNotStr[PrimitiveWithDefaultsFields] | None = None,
        query: str | None = None,
        search_property: (
            PrimitiveWithDefaultsTextFields | SequenceNotStr[PrimitiveWithDefaultsTextFields] | None
        ) = None,
        min_auto_increment_int_32: int | None = None,
        max_auto_increment_int_32: int | None = None,
        default_boolean: bool | None = None,
        min_default_float_32: float | None = None,
        max_default_float_32: float | None = None,
        default_string: str | list[str] | None = None,
        default_string_prefix: str | None = None,
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
        group_by: PrimitiveWithDefaultsFields | SequenceNotStr[PrimitiveWithDefaultsFields],
        property: PrimitiveWithDefaultsFields | SequenceNotStr[PrimitiveWithDefaultsFields] | None = None,
        query: str | None = None,
        search_property: (
            PrimitiveWithDefaultsTextFields | SequenceNotStr[PrimitiveWithDefaultsTextFields] | None
        ) = None,
        min_auto_increment_int_32: int | None = None,
        max_auto_increment_int_32: int | None = None,
        default_boolean: bool | None = None,
        min_default_float_32: float | None = None,
        max_default_float_32: float | None = None,
        default_string: str | list[str] | None = None,
        default_string_prefix: str | None = None,
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
        group_by: PrimitiveWithDefaultsFields | SequenceNotStr[PrimitiveWithDefaultsFields] | None = None,
        property: PrimitiveWithDefaultsFields | SequenceNotStr[PrimitiveWithDefaultsFields] | None = None,
        query: str | None = None,
        search_property: (
            PrimitiveWithDefaultsTextFields | SequenceNotStr[PrimitiveWithDefaultsTextFields] | None
        ) = None,
        min_auto_increment_int_32: int | None = None,
        max_auto_increment_int_32: int | None = None,
        default_boolean: bool | None = None,
        min_default_float_32: float | None = None,
        max_default_float_32: float | None = None,
        default_string: str | list[str] | None = None,
        default_string_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across primitive with defaults

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_auto_increment_int_32: The minimum value of the auto increment int 32 to filter on.
            max_auto_increment_int_32: The maximum value of the auto increment int 32 to filter on.
            default_boolean: The default boolean to filter on.
            min_default_float_32: The minimum value of the default float 32 to filter on.
            max_default_float_32: The maximum value of the default float 32 to filter on.
            default_string: The default string to filter on.
            default_string_prefix: The prefix of the default string to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive with defaults to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count primitive with defaults in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.primitive_with_defaults.aggregate("count", space="my_space")

        """

        filter_ = _create_primitive_with_default_filter(
            self._view_id,
            min_auto_increment_int_32,
            max_auto_increment_int_32,
            default_boolean,
            min_default_float_32,
            max_default_float_32,
            default_string,
            default_string_prefix,
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
        property: PrimitiveWithDefaultsFields,
        interval: float,
        query: str | None = None,
        search_property: (
            PrimitiveWithDefaultsTextFields | SequenceNotStr[PrimitiveWithDefaultsTextFields] | None
        ) = None,
        min_auto_increment_int_32: int | None = None,
        max_auto_increment_int_32: int | None = None,
        default_boolean: bool | None = None,
        min_default_float_32: float | None = None,
        max_default_float_32: float | None = None,
        default_string: str | list[str] | None = None,
        default_string_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for primitive with defaults

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_auto_increment_int_32: The minimum value of the auto increment int 32 to filter on.
            max_auto_increment_int_32: The maximum value of the auto increment int 32 to filter on.
            default_boolean: The default boolean to filter on.
            min_default_float_32: The minimum value of the default float 32 to filter on.
            max_default_float_32: The maximum value of the default float 32 to filter on.
            default_string: The default string to filter on.
            default_string_prefix: The prefix of the default string to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive with defaults to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_primitive_with_default_filter(
            self._view_id,
            min_auto_increment_int_32,
            max_auto_increment_int_32,
            default_boolean,
            min_default_float_32,
            max_default_float_32,
            default_string,
            default_string_prefix,
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

    def query(self) -> PrimitiveWithDefaultsQuery:
        """Start a query for primitive with defaults."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return PrimitiveWithDefaultsQuery(self._client)

    def select(self) -> PrimitiveWithDefaultsQuery:
        """Start selecting from primitive with defaults."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return PrimitiveWithDefaultsQuery(self._client)

    def list(
        self,
        min_auto_increment_int_32: int | None = None,
        max_auto_increment_int_32: int | None = None,
        default_boolean: bool | None = None,
        min_default_float_32: float | None = None,
        max_default_float_32: float | None = None,
        default_string: str | list[str] | None = None,
        default_string_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PrimitiveWithDefaultsFields | Sequence[PrimitiveWithDefaultsFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PrimitiveWithDefaultsList:
        """List/filter primitive with defaults

        Args:
            min_auto_increment_int_32: The minimum value of the auto increment int 32 to filter on.
            max_auto_increment_int_32: The maximum value of the auto increment int 32 to filter on.
            default_boolean: The default boolean to filter on.
            min_default_float_32: The minimum value of the default float 32 to filter on.
            max_default_float_32: The maximum value of the default float 32 to filter on.
            default_string: The default string to filter on.
            default_string_prefix: The prefix of the default string to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive with defaults to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested primitive with defaults

        Examples:

            List primitive with defaults and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_with_defaults = client.primitive_with_defaults.list(limit=5)

        """
        filter_ = _create_primitive_with_default_filter(
            self._view_id,
            min_auto_increment_int_32,
            max_auto_increment_int_32,
            default_boolean,
            min_default_float_32,
            max_default_float_32,
            default_string,
            default_string_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )
