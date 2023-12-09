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
    Meta,
    MetaApply,
    MetaFields,
    MetaList,
    MetaApplyList,
    MetaTextFields,
)
from osdu_wells.client.data_classes._meta import (
    _META_PROPERTIES_BY_FIELD,
    _create_meta_filter,
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
from .meta_query import MetaQueryAPI


class MetaAPI(NodeAPI[Meta, MetaApply, MetaList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[MetaApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Meta,
            class_apply_type=MetaApply,
            class_list=MetaList,
            class_apply_list=MetaApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> MetaQueryAPI[MetaList]:
        """Query starting at metas.

        Args:
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            persistable_reference: The persistable reference to filter on.
            persistable_reference_prefix: The prefix of the persistable reference to filter on.
            unit_of_measure_id: The unit of measure id to filter on.
            unit_of_measure_id_prefix: The prefix of the unit of measure id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for metas.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_meta_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(MetaList)
        return MetaQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, meta: MetaApply | Sequence[MetaApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) metas.

        Args:
            meta: Meta or sequence of metas to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new meta:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import MetaApply
                >>> client = OSDUClient()
                >>> meta = MetaApply(external_id="my_meta", ...)
                >>> result = client.meta.apply(meta)

        """
        return self._apply(meta, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more meta.

        Args:
            external_id: External id of the meta to delete.
            space: The space where all the meta are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete meta by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.meta.delete("my_meta")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Meta | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> MetaList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Meta | MetaList | None:
        """Retrieve one or more metas by id(s).

        Args:
            external_id: External id or list of external ids of the metas.
            space: The space where all the metas are located.

        Returns:
            The requested metas.

        Examples:

            Retrieve meta by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> meta = client.meta.retrieve("my_meta")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MetaList:
        """Search metas

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            persistable_reference: The persistable reference to filter on.
            persistable_reference_prefix: The prefix of the persistable reference to filter on.
            unit_of_measure_id: The unit of measure id to filter on.
            unit_of_measure_id_prefix: The prefix of the unit of measure id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results metas matching the query.

        Examples:

           Search for 'my_meta' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> metas = client.meta.search('my_meta')

        """
        filter_ = _create_meta_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _META_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MetaFields | Sequence[MetaFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
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
        property: MetaFields | Sequence[MetaFields] | None = None,
        group_by: MetaFields | Sequence[MetaFields] = None,
        query: str | None = None,
        search_properties: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
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
        property: MetaFields | Sequence[MetaFields] | None = None,
        group_by: MetaFields | Sequence[MetaFields] | None = None,
        query: str | None = None,
        search_property: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across metas

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            persistable_reference: The persistable reference to filter on.
            persistable_reference_prefix: The prefix of the persistable reference to filter on.
            unit_of_measure_id: The unit of measure id to filter on.
            unit_of_measure_id_prefix: The prefix of the unit of measure id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count metas in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.meta.aggregate("count", space="my_space")

        """

        filter_ = _create_meta_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _META_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MetaFields,
        interval: float,
        query: str | None = None,
        search_property: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for metas

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            persistable_reference: The persistable reference to filter on.
            persistable_reference_prefix: The prefix of the persistable reference to filter on.
            unit_of_measure_id: The unit of measure id to filter on.
            unit_of_measure_id_prefix: The prefix of the unit of measure id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_meta_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _META_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MetaList:
        """List/filter metas

        Args:
            kind: The kind to filter on.
            kind_prefix: The prefix of the kind to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            persistable_reference: The persistable reference to filter on.
            persistable_reference_prefix: The prefix of the persistable reference to filter on.
            unit_of_measure_id: The unit of measure id to filter on.
            unit_of_measure_id_prefix: The prefix of the unit of measure id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested metas

        Examples:

            List metas and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> metas = client.meta.list(limit=5)

        """
        filter_ = _create_meta_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
