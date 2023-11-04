from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from markets_pydantic_v1.client.data_classes import (
    PygenProcess,
    PygenProcessApply,
    PygenProcessList,
    PygenProcessApplyList,
    PygenProcessFields,
    PygenProcessTextFields,
    DomainModelApply,
)
from markets_pydantic_v1.client.data_classes._pygen_process import _PYGENPROCESS_PROPERTIES_BY_FIELD


class PygenProcessAPI(TypeAPI[PygenProcess, PygenProcessApply, PygenProcessList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[PygenProcessApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PygenProcess,
            class_apply_type=PygenProcessApply,
            class_list=PygenProcessList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, pygen_proces: PygenProcessApply | Sequence[PygenProcessApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) pygen process.

        Args:
            pygen_proces: Pygen proces or sequence of pygen process to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new pygen_proces:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> from markets_pydantic_v1.client.data_classes import PygenProcessApply
                >>> client = MarketClient()
                >>> pygen_proces = PygenProcessApply(external_id="my_pygen_proces", ...)
                >>> result = client.pygen_process.apply(pygen_proces)

        """
        if isinstance(pygen_proces, PygenProcessApply):
            instances = pygen_proces.to_instances_apply(self._view_by_write_class)
        else:
            instances = PygenProcessApplyList(pygen_proces).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space: str = "market") -> dm.InstancesDeleteResult:
        """Delete one or more pygen proces.

        Args:
            external_id: External id of the pygen proces to delete.
            space: The space where all the pygen proces are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete pygen_proces by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> client.pygen_process.delete("my_pygen_proces")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> PygenProcess:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PygenProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "market") -> PygenProcess | PygenProcessList:
        """Retrieve one or more pygen process by id(s).

        Args:
            external_id: External id or list of external ids of the pygen process.
            space: The space where all the pygen process are located.

        Returns:
            The requested pygen process.

        Examples:

            Retrieve pygen_proces by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> pygen_proces = client.pygen_process.retrieve("my_pygen_proces")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: PygenProcessTextFields | Sequence[PygenProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PygenProcessList:
        """Search pygen process

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
            limit: Maximum number of pygen process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results pygen process matching the query.

        Examples:

           Search for 'my_pygen_proces' in all text properties:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> pygen_process = client.pygen_process.search('my_pygen_proces')

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
        return self._search(self._view_id, query, _PYGENPROCESS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PygenProcessFields | Sequence[PygenProcessFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PygenProcessTextFields | Sequence[PygenProcessTextFields] | None = None,
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
        property: PygenProcessFields | Sequence[PygenProcessFields] | None = None,
        group_by: PygenProcessFields | Sequence[PygenProcessFields] = None,
        query: str | None = None,
        search_properties: PygenProcessTextFields | Sequence[PygenProcessTextFields] | None = None,
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
        property: PygenProcessFields | Sequence[PygenProcessFields] | None = None,
        group_by: PygenProcessFields | Sequence[PygenProcessFields] | None = None,
        query: str | None = None,
        search_property: PygenProcessTextFields | Sequence[PygenProcessTextFields] | None = None,
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
        """Aggregate data across pygen process

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
            limit: Maximum number of pygen process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count pygen process in space `my_space`:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.pygen_process.aggregate("count", space="my_space")

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
            _PYGENPROCESS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PygenProcessFields,
        interval: float,
        query: str | None = None,
        search_property: PygenProcessTextFields | Sequence[PygenProcessTextFields] | None = None,
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
        """Produces histograms for pygen process

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
            limit: Maximum number of pygen process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
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
            _PYGENPROCESS_PROPERTIES_BY_FIELD,
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
    ) -> PygenProcessList:
        """List/filter pygen process

        Args:
            bid: The bid to filter on.
            date_transformations: The date transformation to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            transformation: The transformation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested pygen process

        Examples:

            List pygen process and limit to 5:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> pygen_process = client.pygen_process.list(limit=5)

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
