from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    Ancestry,
    AncestryApply,
    AncestryList,
    AncestryApplyList,
    AncestryFields,
    AncestryTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._ancestry import _ANCESTRY_PROPERTIES_BY_FIELD


class AncestryAPI(TypeAPI[Ancestry, AncestryApply, AncestryList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AncestryApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Ancestry,
            class_apply_type=AncestryApply,
            class_list=AncestryList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, ancestry: AncestryApply | Sequence[AncestryApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) ancestries.

        Args:
            ancestry: Ancestry or sequence of ancestries to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new ancestry:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import AncestryApply
                >>> client = OSDUClient()
                >>> ancestry = AncestryApply(external_id="my_ancestry", ...)
                >>> result = client.ancestry.apply(ancestry)

        """
        if isinstance(ancestry, AncestryApply):
            instances = ancestry.to_instances_apply(self._view_by_write_class)
        else:
            instances = AncestryApplyList(ancestry).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more ancestry.

        Args:
            external_id: External id of the ancestry to delete.
            space: The space where all the ancestry are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete ancestry by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.ancestry.delete("my_ancestry")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Ancestry:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> AncestryList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> Ancestry | AncestryList:
        """Retrieve one or more ancestries by id(s).

        Args:
            external_id: External id or list of external ids of the ancestries.
            space: The space where all the ancestries are located.

        Returns:
            The requested ancestries.

        Examples:

            Retrieve ancestry by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> ancestry = client.ancestry.retrieve("my_ancestry")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: AncestryTextFields | Sequence[AncestryTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AncestryList:
        """Search ancestries

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of ancestries to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results ancestries matching the query.

        Examples:

           Search for 'my_ancestry' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> ancestries = client.ancestry.search('my_ancestry')

        """
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _ANCESTRY_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AncestryFields | Sequence[AncestryFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AncestryTextFields | Sequence[AncestryTextFields] | None = None,
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
        property: AncestryFields | Sequence[AncestryFields] | None = None,
        group_by: AncestryFields | Sequence[AncestryFields] = None,
        query: str | None = None,
        search_properties: AncestryTextFields | Sequence[AncestryTextFields] | None = None,
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
        property: AncestryFields | Sequence[AncestryFields] | None = None,
        group_by: AncestryFields | Sequence[AncestryFields] | None = None,
        query: str | None = None,
        search_property: AncestryTextFields | Sequence[AncestryTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across ancestries

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of ancestries to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count ancestries in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.ancestry.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ANCESTRY_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AncestryFields,
        interval: float,
        query: str | None = None,
        search_property: AncestryTextFields | Sequence[AncestryTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for ancestries

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of ancestries to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ANCESTRY_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AncestryList:
        """List/filter ancestries

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of ancestries to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested ancestries

        Examples:

            List ancestries and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> ancestries = client.ancestry.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
