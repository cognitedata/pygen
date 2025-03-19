from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
)
from omni.data_classes._core.query import (
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite_core.data_classes._cognite_unit import (
    CogniteUnitQuery,
    _COGNITEUNIT_PROPERTIES_BY_FIELD,
    _create_cognite_unit_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CogniteUnit,
    CogniteUnitWrite,
    CogniteUnitFields,
    CogniteUnitList,
    CogniteUnitWriteList,
    CogniteUnitTextFields,
)


class CogniteUnitAPI(NodeAPI[CogniteUnit, CogniteUnitWrite, CogniteUnitList, CogniteUnitWriteList]):
    _view_id = dm.ViewId("cdf_cdm", "CogniteUnit", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITEUNIT_PROPERTIES_BY_FIELD
    _class_type = CogniteUnit
    _class_list = CogniteUnitList
    _class_write_list = CogniteUnitWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteUnit | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteUnitList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteUnit | CogniteUnitList | None:
        """Retrieve one or more Cognite units by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite units.
            space: The space where all the Cognite units are located.

        Returns:
            The requested Cognite units.

        Examples:

            Retrieve cognite_unit by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_unit = client.cognite_unit.retrieve(
                ...     "my_cognite_unit"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: CogniteUnitTextFields | SequenceNotStr[CogniteUnitTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        quantity: str | list[str] | None = None,
        quantity_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        source_reference: str | list[str] | None = None,
        source_reference_prefix: str | None = None,
        symbol: str | list[str] | None = None,
        symbol_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteUnitFields | SequenceNotStr[CogniteUnitFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteUnitList:
        """Search Cognite units

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            quantity: The quantity to filter on.
            quantity_prefix: The prefix of the quantity to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            source_reference: The source reference to filter on.
            source_reference_prefix: The prefix of the source reference to filter on.
            symbol: The symbol to filter on.
            symbol_prefix: The prefix of the symbol to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite units to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite units matching the query.

        Examples:

           Search for 'my_cognite_unit' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_units = client.cognite_unit.search(
                ...     'my_cognite_unit'
                ... )

        """
        filter_ = _create_cognite_unit_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            quantity,
            quantity_prefix,
            source,
            source_prefix,
            source_reference,
            source_reference_prefix,
            symbol,
            symbol_prefix,
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
        property: CogniteUnitFields | SequenceNotStr[CogniteUnitFields] | None = None,
        query: str | None = None,
        search_property: CogniteUnitTextFields | SequenceNotStr[CogniteUnitTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        quantity: str | list[str] | None = None,
        quantity_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        source_reference: str | list[str] | None = None,
        source_reference_prefix: str | None = None,
        symbol: str | list[str] | None = None,
        symbol_prefix: str | None = None,
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
        property: CogniteUnitFields | SequenceNotStr[CogniteUnitFields] | None = None,
        query: str | None = None,
        search_property: CogniteUnitTextFields | SequenceNotStr[CogniteUnitTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        quantity: str | list[str] | None = None,
        quantity_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        source_reference: str | list[str] | None = None,
        source_reference_prefix: str | None = None,
        symbol: str | list[str] | None = None,
        symbol_prefix: str | None = None,
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
        group_by: CogniteUnitFields | SequenceNotStr[CogniteUnitFields],
        property: CogniteUnitFields | SequenceNotStr[CogniteUnitFields] | None = None,
        query: str | None = None,
        search_property: CogniteUnitTextFields | SequenceNotStr[CogniteUnitTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        quantity: str | list[str] | None = None,
        quantity_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        source_reference: str | list[str] | None = None,
        source_reference_prefix: str | None = None,
        symbol: str | list[str] | None = None,
        symbol_prefix: str | None = None,
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
        group_by: CogniteUnitFields | SequenceNotStr[CogniteUnitFields] | None = None,
        property: CogniteUnitFields | SequenceNotStr[CogniteUnitFields] | None = None,
        query: str | None = None,
        search_property: CogniteUnitTextFields | SequenceNotStr[CogniteUnitTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        quantity: str | list[str] | None = None,
        quantity_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        source_reference: str | list[str] | None = None,
        source_reference_prefix: str | None = None,
        symbol: str | list[str] | None = None,
        symbol_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite units

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            quantity: The quantity to filter on.
            quantity_prefix: The prefix of the quantity to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            source_reference: The source reference to filter on.
            source_reference_prefix: The prefix of the source reference to filter on.
            symbol: The symbol to filter on.
            symbol_prefix: The prefix of the symbol to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite units to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite units in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_unit.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_unit_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            quantity,
            quantity_prefix,
            source,
            source_prefix,
            source_reference,
            source_reference_prefix,
            symbol,
            symbol_prefix,
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
        property: CogniteUnitFields,
        interval: float,
        query: str | None = None,
        search_property: CogniteUnitTextFields | SequenceNotStr[CogniteUnitTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        quantity: str | list[str] | None = None,
        quantity_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        source_reference: str | list[str] | None = None,
        source_reference_prefix: str | None = None,
        symbol: str | list[str] | None = None,
        symbol_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite units

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            quantity: The quantity to filter on.
            quantity_prefix: The prefix of the quantity to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            source_reference: The source reference to filter on.
            source_reference_prefix: The prefix of the source reference to filter on.
            symbol: The symbol to filter on.
            symbol_prefix: The prefix of the symbol to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite units to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_unit_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            quantity,
            quantity_prefix,
            source,
            source_prefix,
            source_reference,
            source_reference_prefix,
            symbol,
            symbol_prefix,
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

    def select(self) -> CogniteUnitQuery:
        """Start selecting from Cognite units."""
        return CogniteUnitQuery(self._client)

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
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        quantity: str | list[str] | None = None,
        quantity_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        source_reference: str | list[str] | None = None,
        source_reference_prefix: str | None = None,
        symbol: str | list[str] | None = None,
        symbol_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteUnitFields | Sequence[CogniteUnitFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteUnitList:
        """List/filter Cognite units

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            quantity: The quantity to filter on.
            quantity_prefix: The prefix of the quantity to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            source_reference: The source reference to filter on.
            source_reference_prefix: The prefix of the source reference to filter on.
            symbol: The symbol to filter on.
            symbol_prefix: The prefix of the symbol to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite units to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested Cognite units

        Examples:

            List Cognite units and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_units = client.cognite_unit.list(limit=5)

        """
        filter_ = _create_cognite_unit_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            quantity,
            quantity_prefix,
            source,
            source_prefix,
            source_reference,
            source_reference_prefix,
            symbol,
            symbol_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
