from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from omni.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    PrimitiveWithDefaults,
    PrimitiveWithDefaultsApply,
    PrimitiveWithDefaultsFields,
    PrimitiveWithDefaultsList,
    PrimitiveWithDefaultsApplyList,
    PrimitiveWithDefaultsTextFields,
)
from omni.data_classes._primitive_with_defaults import (
    _PRIMITIVEWITHDEFAULTS_PROPERTIES_BY_FIELD,
    _create_primitive_with_default_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .primitive_with_defaults_query import PrimitiveWithDefaultsQueryAPI


class PrimitiveWithDefaultsAPI(NodeAPI[PrimitiveWithDefaults, PrimitiveWithDefaultsApply, PrimitiveWithDefaultsList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[PrimitiveWithDefaults]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PrimitiveWithDefaults,
            class_list=PrimitiveWithDefaultsList,
            class_apply_list=PrimitiveWithDefaultsApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

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
        builder = QueryBuilder(PrimitiveWithDefaultsList)
        return PrimitiveWithDefaultsQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        primitive_with_default: PrimitiveWithDefaultsApply | Sequence[PrimitiveWithDefaultsApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) primitive with defaults.

        Args:
            primitive_with_default: Primitive with default or sequence of primitive with defaults to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): Should we write None values to the API? If False, None values will be ignored. If True, None values will be written to the API.
                Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new primitive_with_default:

                >>> from omni import OmniClient
                >>> from omni.data_classes import PrimitiveWithDefaultsApply
                >>> client = OmniClient()
                >>> primitive_with_default = PrimitiveWithDefaultsApply(external_id="my_primitive_with_default", ...)
                >>> result = client.primitive_with_defaults.apply(primitive_with_default)

        """
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
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PrimitiveWithDefaults | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveWithDefaultsList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
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
        properties: PrimitiveWithDefaultsTextFields | Sequence[PrimitiveWithDefaultsTextFields] | None = None,
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
            self._view_id, query, _PRIMITIVEWITHDEFAULTS_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveWithDefaultsFields | Sequence[PrimitiveWithDefaultsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PrimitiveWithDefaultsTextFields | Sequence[PrimitiveWithDefaultsTextFields] | None = None,
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
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveWithDefaultsFields | Sequence[PrimitiveWithDefaultsFields] | None = None,
        group_by: PrimitiveWithDefaultsFields | Sequence[PrimitiveWithDefaultsFields] = None,
        query: str | None = None,
        search_properties: PrimitiveWithDefaultsTextFields | Sequence[PrimitiveWithDefaultsTextFields] | None = None,
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
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveWithDefaultsFields | Sequence[PrimitiveWithDefaultsFields] | None = None,
        group_by: PrimitiveWithDefaultsFields | Sequence[PrimitiveWithDefaultsFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveWithDefaultsTextFields | Sequence[PrimitiveWithDefaultsTextFields] | None = None,
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
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across primitive with defaults

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
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
            self._view_id,
            aggregate,
            _PRIMITIVEWITHDEFAULTS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PrimitiveWithDefaultsFields,
        interval: float,
        query: str | None = None,
        search_property: PrimitiveWithDefaultsTextFields | Sequence[PrimitiveWithDefaultsTextFields] | None = None,
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
            self._view_id,
            property,
            interval,
            _PRIMITIVEWITHDEFAULTS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

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
        return self._list(limit=limit, filter=filter_)
