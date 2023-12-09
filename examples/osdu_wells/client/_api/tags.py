from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Tags,
    TagsApply,
    TagsFields,
    TagsList,
    TagsApplyList,
    TagsTextFields,
)
from osdu_wells.client.data_classes._tags import (
    _TAGS_PROPERTIES_BY_FIELD,
    _create_tag_filter,
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
from .tags_query import TagsQueryAPI


class TagsAPI(NodeAPI[Tags, TagsApply, TagsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[TagsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Tags,
            class_apply_type=TagsApply,
            class_list=TagsList,
            class_apply_list=TagsApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name_of_key: str | list[str] | None = None,
        name_of_key_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> TagsQueryAPI[TagsList]:
        """Query starting at tags.

        Args:
            name_of_key: The name of key to filter on.
            name_of_key_prefix: The prefix of the name of key to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of tags to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for tags.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_tag_filter(
            self._view_id,
            name_of_key,
            name_of_key_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(TagsList)
        return TagsQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, tag: TagsApply | Sequence[TagsApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) tags.

        Args:
            tag: Tag or sequence of tags to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new tag:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import TagsApply
                >>> client = OSDUClient()
                >>> tag = TagsApply(external_id="my_tag", ...)
                >>> result = client.tags.apply(tag)

        """
        return self._apply(tag, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more tag.

        Args:
            external_id: External id of the tag to delete.
            space: The space where all the tag are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete tag by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.tags.delete("my_tag")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Tags | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> TagsList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Tags | TagsList | None:
        """Retrieve one or more tags by id(s).

        Args:
            external_id: External id or list of external ids of the tags.
            space: The space where all the tags are located.

        Returns:
            The requested tags.

        Examples:

            Retrieve tag by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> tag = client.tags.retrieve("my_tag")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: TagsTextFields | Sequence[TagsTextFields] | None = None,
        name_of_key: str | list[str] | None = None,
        name_of_key_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TagsList:
        """Search tags

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name_of_key: The name of key to filter on.
            name_of_key_prefix: The prefix of the name of key to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of tags to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results tags matching the query.

        Examples:

           Search for 'my_tag' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> tags = client.tags.search('my_tag')

        """
        filter_ = _create_tag_filter(
            self._view_id,
            name_of_key,
            name_of_key_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _TAGS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: TagsFields | Sequence[TagsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: TagsTextFields | Sequence[TagsTextFields] | None = None,
        name_of_key: str | list[str] | None = None,
        name_of_key_prefix: str | None = None,
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
        property: TagsFields | Sequence[TagsFields] | None = None,
        group_by: TagsFields | Sequence[TagsFields] = None,
        query: str | None = None,
        search_properties: TagsTextFields | Sequence[TagsTextFields] | None = None,
        name_of_key: str | list[str] | None = None,
        name_of_key_prefix: str | None = None,
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
        property: TagsFields | Sequence[TagsFields] | None = None,
        group_by: TagsFields | Sequence[TagsFields] | None = None,
        query: str | None = None,
        search_property: TagsTextFields | Sequence[TagsTextFields] | None = None,
        name_of_key: str | list[str] | None = None,
        name_of_key_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across tags

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name_of_key: The name of key to filter on.
            name_of_key_prefix: The prefix of the name of key to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of tags to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count tags in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.tags.aggregate("count", space="my_space")

        """

        filter_ = _create_tag_filter(
            self._view_id,
            name_of_key,
            name_of_key_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _TAGS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: TagsFields,
        interval: float,
        query: str | None = None,
        search_property: TagsTextFields | Sequence[TagsTextFields] | None = None,
        name_of_key: str | list[str] | None = None,
        name_of_key_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for tags

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name_of_key: The name of key to filter on.
            name_of_key_prefix: The prefix of the name of key to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of tags to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_tag_filter(
            self._view_id,
            name_of_key,
            name_of_key_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _TAGS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name_of_key: str | list[str] | None = None,
        name_of_key_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TagsList:
        """List/filter tags

        Args:
            name_of_key: The name of key to filter on.
            name_of_key_prefix: The prefix of the name of key to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of tags to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested tags

        Examples:

            List tags and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> tags = client.tags.list(limit=5)

        """
        filter_ = _create_tag_filter(
            self._view_id,
            name_of_key,
            name_of_key_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
