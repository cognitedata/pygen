from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni_pydantic_v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni_pydantic_v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PrimitiveRequiredListed,
    PrimitiveRequiredListedWrite,
    PrimitiveRequiredListedFields,
    PrimitiveRequiredListedList,
    PrimitiveRequiredListedWriteList,
    PrimitiveRequiredListedTextFields,
)
from omni_pydantic_v1.data_classes._primitive_required_listed import (
    _PRIMITIVEREQUIREDLISTED_PROPERTIES_BY_FIELD,
    _create_primitive_required_listed_filter,
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
from .primitive_required_listed_query import PrimitiveRequiredListedQueryAPI


class PrimitiveRequiredListedAPI(
    NodeAPI[PrimitiveRequiredListed, PrimitiveRequiredListedWrite, PrimitiveRequiredListedList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[PrimitiveRequiredListed]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PrimitiveRequiredListed,
            class_list=PrimitiveRequiredListedList,
            class_write_list=PrimitiveRequiredListedWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PrimitiveRequiredListedQueryAPI[PrimitiveRequiredListedList]:
        """Query starting at primitive required listeds.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive required listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for primitive required listeds.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_primitive_required_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PrimitiveRequiredListedList)
        return PrimitiveRequiredListedQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        primitive_required_listed: PrimitiveRequiredListedWrite | Sequence[PrimitiveRequiredListedWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) primitive required listeds.

        Args:
            primitive_required_listed: Primitive required listed or sequence of primitive required listeds to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new primitive_required_listed:

                >>> from omni_pydantic_v1 import OmniClient
                >>> from omni_pydantic_v1.data_classes import PrimitiveRequiredListedWrite
                >>> client = OmniClient()
                >>> primitive_required_listed = PrimitiveRequiredListedWrite(external_id="my_primitive_required_listed", ...)
                >>> result = client.primitive_required_listed.apply(primitive_required_listed)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.primitive_required_listed.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(primitive_required_listed, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more primitive required listed.

        Args:
            external_id: External id of the primitive required listed to delete.
            space: The space where all the primitive required listed are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete primitive_required_listed by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> client.primitive_required_listed.delete("my_primitive_required_listed")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.primitive_required_listed.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PrimitiveRequiredListed | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveRequiredListedList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveRequiredListed | PrimitiveRequiredListedList | None:
        """Retrieve one or more primitive required listeds by id(s).

        Args:
            external_id: External id or list of external ids of the primitive required listeds.
            space: The space where all the primitive required listeds are located.

        Returns:
            The requested primitive required listeds.

        Examples:

            Retrieve primitive_required_listed by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> primitive_required_listed = client.primitive_required_listed.retrieve("my_primitive_required_listed")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PrimitiveRequiredListedTextFields | Sequence[PrimitiveRequiredListedTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PrimitiveRequiredListedFields | Sequence[PrimitiveRequiredListedFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PrimitiveRequiredListedList:
        """Search primitive required listeds

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive required listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results primitive required listeds matching the query.

        Examples:

           Search for 'my_primitive_required_listed' in all text properties:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> primitive_required_listeds = client.primitive_required_listed.search('my_primitive_required_listed')

        """
        filter_ = _create_primitive_required_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            view_id=self._view_id,
            query=query,
            properties_by_field=_PRIMITIVEREQUIREDLISTED_PROPERTIES_BY_FIELD,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: PrimitiveRequiredListedFields | Sequence[PrimitiveRequiredListedFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: (
            PrimitiveRequiredListedTextFields | Sequence[PrimitiveRequiredListedTextFields] | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: PrimitiveRequiredListedFields | Sequence[PrimitiveRequiredListedFields] | None = None,
        group_by: PrimitiveRequiredListedFields | Sequence[PrimitiveRequiredListedFields] = None,
        query: str | None = None,
        search_properties: (
            PrimitiveRequiredListedTextFields | Sequence[PrimitiveRequiredListedTextFields] | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: PrimitiveRequiredListedFields | Sequence[PrimitiveRequiredListedFields] | None = None,
        group_by: PrimitiveRequiredListedFields | Sequence[PrimitiveRequiredListedFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveRequiredListedTextFields | Sequence[PrimitiveRequiredListedTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across primitive required listeds

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive required listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count primitive required listeds in space `my_space`:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> result = client.primitive_required_listed.aggregate("count", space="my_space")

        """

        filter_ = _create_primitive_required_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PRIMITIVEREQUIREDLISTED_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PrimitiveRequiredListedFields,
        interval: float,
        query: str | None = None,
        search_property: PrimitiveRequiredListedTextFields | Sequence[PrimitiveRequiredListedTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for primitive required listeds

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive required listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_primitive_required_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PRIMITIVEREQUIREDLISTED_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PrimitiveRequiredListedFields | Sequence[PrimitiveRequiredListedFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PrimitiveRequiredListedList:
        """List/filter primitive required listeds

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive required listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested primitive required listeds

        Examples:

            List primitive required listeds and limit to 5:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> primitive_required_listeds = client.primitive_required_listed.list(limit=5)

        """
        filter_ = _create_primitive_required_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(
            limit=limit,
            filter=filter_,
            properties_by_field=_PRIMITIVEREQUIREDLISTED_PROPERTIES_BY_FIELD,
            sort_by=sort_by,
            direction=direction,
            sort=sort,
        )
