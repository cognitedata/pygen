from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni._api._core import (
    DEFAULT_LIMIT_READ,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from omni.data_classes._cdf_external_references_listed import (
    CDFExternalReferencesListedQuery,
    _CDFEXTERNALREFERENCESLISTED_PROPERTIES_BY_FIELD,
    _create_cdf_external_references_listed_filter,
)
from omni.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CDFExternalReferencesListed,
    CDFExternalReferencesListedWrite,
    CDFExternalReferencesListedFields,
    CDFExternalReferencesListedList,
    CDFExternalReferencesListedWriteList,
    CDFExternalReferencesListedTextFields,
)


class CDFExternalReferencesListedAPI(
    NodeAPI[
        CDFExternalReferencesListed,
        CDFExternalReferencesListedWrite,
        CDFExternalReferencesListedList,
        CDFExternalReferencesListedWriteList,
    ]
):
    _view_id = dm.ViewId("sp_pygen_models", "CDFExternalReferencesListed", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _CDFEXTERNALREFERENCESLISTED_PROPERTIES_BY_FIELD
    _class_type = CDFExternalReferencesListed
    _class_list = CDFExternalReferencesListedList
    _class_write_list = CDFExternalReferencesListedWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CDFExternalReferencesListed | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CDFExternalReferencesListedList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CDFExternalReferencesListed | CDFExternalReferencesListedList | None:
        """Retrieve one or more cdf external references listeds by id(s).

        Args:
            external_id: External id or list of external ids of the cdf external references listeds.
            space: The space where all the cdf external references listeds are located.

        Returns:
            The requested cdf external references listeds.

        Examples:

            Retrieve cdf_external_references_listed by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_references_listed = client.cdf_external_references_listed.retrieve(
                ...     "my_cdf_external_references_listed"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: (
            CDFExternalReferencesListedTextFields | SequenceNotStr[CDFExternalReferencesListedTextFields] | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CDFExternalReferencesListedFields | SequenceNotStr[CDFExternalReferencesListedFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CDFExternalReferencesListedList:
        """Search cdf external references listeds

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listeds to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results cdf external references listeds matching the query.

        Examples:

           Search for 'my_cdf_external_references_listed' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_references_listeds = client.cdf_external_references_listed.search(
                ...     'my_cdf_external_references_listed'
                ... )

        """
        filter_ = _create_cdf_external_references_listed_filter(
            self._view_id,
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
        property: CDFExternalReferencesListedFields | SequenceNotStr[CDFExternalReferencesListedFields] | None = None,
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
        property: CDFExternalReferencesListedFields | SequenceNotStr[CDFExternalReferencesListedFields] | None = None,
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
        group_by: CDFExternalReferencesListedFields | SequenceNotStr[CDFExternalReferencesListedFields],
        property: CDFExternalReferencesListedFields | SequenceNotStr[CDFExternalReferencesListedFields] | None = None,
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
        group_by: CDFExternalReferencesListedFields | SequenceNotStr[CDFExternalReferencesListedFields] | None = None,
        property: CDFExternalReferencesListedFields | SequenceNotStr[CDFExternalReferencesListedFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across cdf external references listeds

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listeds to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cdf external references listeds in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.cdf_external_references_listed.aggregate("count", space="my_space")

        """

        filter_ = _create_cdf_external_references_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=None,
            search_properties=None,
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: CDFExternalReferencesListedFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cdf external references listeds

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listeds to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cdf_external_references_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            None,
            None,
            limit,
            filter_,
        )

    def select(self) -> CDFExternalReferencesListedQuery:
        """Start selecting from cdf external references listeds."""
        return CDFExternalReferencesListedQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> list[dict[str, Any]]:
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
        return QueryUnpacker(builder, edges=unpack_edges).unpack()

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CDFExternalReferencesListedFields | Sequence[CDFExternalReferencesListedFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CDFExternalReferencesListedList:
        """List/filter cdf external references listeds

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references listeds to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested cdf external references listeds

        Examples:

            List cdf external references listeds and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_references_listeds = client.cdf_external_references_listed.list(limit=5)

        """
        filter_ = _create_cdf_external_references_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
