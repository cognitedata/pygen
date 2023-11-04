from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from markets_pydantic_v1.client.data_classes import (
    CogProcess,
    CogProcessApply,
    CogProcessList,
    CogProcessApplyList,
    CogProcessFields,
    CogProcessTextFields,
    DomainModelApply,
)
from markets_pydantic_v1.client.data_classes._cog_process import _COGPROCESS_PROPERTIES_BY_FIELD


class CogProcessAPI(TypeAPI[CogProcess, CogProcessApply, CogProcessList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CogProcessApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CogProcess,
            class_apply_type=CogProcessApply,
            class_list=CogProcessList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, cog_proces: CogProcessApply | Sequence[CogProcessApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) cog process.

        Args:
            cog_proces: Cog proces or sequence of cog process to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new cog_proces:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> from markets_pydantic_v1.client.data_classes import CogProcessApply
                >>> client = MarketClient()
                >>> cog_proces = CogProcessApply(external_id="my_cog_proces", ...)
                >>> result = client.cog_process.apply(cog_proces)

        """
        if isinstance(cog_proces, CogProcessApply):
            instances = cog_proces.to_instances_apply(self._view_by_write_class)
        else:
            instances = CogProcessApplyList(cog_proces).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space: str = "market") -> dm.InstancesDeleteResult:
        """Delete one or more cog proces.

        Args:
            external_id: External id of the cog proces to delete.
            space: The space where all the cog proces are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cog_proces by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> client.cog_process.delete("my_cog_proces")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CogProcess:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CogProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "market") -> CogProcess | CogProcessList:
        """Retrieve one or more cog process by id(s).

        Args:
            external_id: External id or list of external ids of the cog process.
            space: The space where all the cog process are located.

        Returns:
            The requested cog process.

        Examples:

            Retrieve cog_proces by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> cog_proces = client.cog_process.retrieve("my_cog_proces")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogProcessList:
        """Search cog process

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            bid: The bid to filter on.
            date_transformations: The date transformation to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            transformation: The transformation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results cog process matching the query.

        Examples:

           Search for 'my_cog_proces' in all text properties:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> cog_process = client.cog_process.search('my_cog_proces')

        """
        filter_ = _create_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _COGPROCESS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CogProcessFields | Sequence[CogProcessFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: CogProcessFields | Sequence[CogProcessFields] | None = None,
        group_by: CogProcessFields | Sequence[CogProcessFields] = None,
        query: str | None = None,
        search_properties: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: CogProcessFields | Sequence[CogProcessFields] | None = None,
        group_by: CogProcessFields | Sequence[CogProcessFields] | None = None,
        query: str | None = None,
        search_property: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across cog process

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            bid: The bid to filter on.
            date_transformations: The date transformation to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            transformation: The transformation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cog process in space `my_space`:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.cog_process.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _COGPROCESS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CogProcessFields,
        interval: float,
        query: str | None = None,
        search_property: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cog process

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            bid: The bid to filter on.
            date_transformations: The date transformation to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            transformation: The transformation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _COGPROCESS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogProcessList:
        """List/filter cog process

        Args:
            bid: The bid to filter on.
            date_transformations: The date transformation to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            transformation: The transformation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested cog process

        Examples:

            List cog process and limit to 5:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> cog_process = client.cog_process.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if bid and isinstance(bid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": "market", "externalId": bid}))
    if bid and isinstance(bid, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": bid[0], "externalId": bid[1]}))
    if bid and isinstance(bid, list) and isinstance(bid[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": "market", "externalId": item} for item in bid]
            )
        )
    if bid and isinstance(bid, list) and isinstance(bid[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": item[0], "externalId": item[1]} for item in bid]
            )
        )
    if date_transformations and isinstance(date_transformations, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("dateTransformations"),
                value={"space": "market", "externalId": date_transformations},
            )
        )
    if date_transformations and isinstance(date_transformations, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("dateTransformations"),
                value={"space": date_transformations[0], "externalId": date_transformations[1]},
            )
        )
    if date_transformations and isinstance(date_transformations, list) and isinstance(date_transformations[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("dateTransformations"),
                values=[{"space": "market", "externalId": item} for item in date_transformations],
            )
        )
    if date_transformations and isinstance(date_transformations, list) and isinstance(date_transformations[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("dateTransformations"),
                values=[{"space": item[0], "externalId": item[1]} for item in date_transformations],
            )
        )
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if transformation and isinstance(transformation, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("transformation"), value={"space": "market", "externalId": transformation}
            )
        )
    if transformation and isinstance(transformation, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("transformation"),
                value={"space": transformation[0], "externalId": transformation[1]},
            )
        )
    if transformation and isinstance(transformation, list) and isinstance(transformation[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("transformation"),
                values=[{"space": "market", "externalId": item} for item in transformation],
            )
        )
    if transformation and isinstance(transformation, list) and isinstance(transformation[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("transformation"),
                values=[{"space": item[0], "externalId": item[1]} for item in transformation],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
