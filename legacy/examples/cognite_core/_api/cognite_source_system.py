from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite_core.data_classes._cognite_source_system import (
    CogniteSourceSystemQuery,
    _COGNITESOURCESYSTEM_PROPERTIES_BY_FIELD,
    _create_cognite_source_system_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CogniteSourceSystem,
    CogniteSourceSystemWrite,
    CogniteSourceSystemFields,
    CogniteSourceSystemList,
    CogniteSourceSystemWriteList,
    CogniteSourceSystemTextFields,
)


class CogniteSourceSystemAPI(
    NodeAPI[CogniteSourceSystem, CogniteSourceSystemWrite, CogniteSourceSystemList, CogniteSourceSystemWriteList]
):
    _view_id = dm.ViewId("cdf_cdm", "CogniteSourceSystem", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITESOURCESYSTEM_PROPERTIES_BY_FIELD
    _class_type = CogniteSourceSystem
    _class_list = CogniteSourceSystemList
    _class_write_list = CogniteSourceSystemWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteSourceSystem | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteSourceSystemList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteSourceSystem | CogniteSourceSystemList | None:
        """Retrieve one or more Cognite source systems by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite source systems.
            space: The space where all the Cognite source systems are located.

        Returns:
            The requested Cognite source systems.

        Examples:

            Retrieve cognite_source_system by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_source_system = client.cognite_source_system.retrieve(
                ...     "my_cognite_source_system"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: CogniteSourceSystemTextFields | SequenceNotStr[CogniteSourceSystemTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteSourceSystemFields | SequenceNotStr[CogniteSourceSystemFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteSourceSystemList:
        """Search Cognite source systems

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            manufacturer: The manufacturer to filter on.
            manufacturer_prefix: The prefix of the manufacturer to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            version_: The version to filter on.
            version_prefix: The prefix of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite source systems to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite source systems matching the query.

        Examples:

           Search for 'my_cognite_source_system' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_source_systems = client.cognite_source_system.search(
                ...     'my_cognite_source_system'
                ... )

        """
        filter_ = _create_cognite_source_system_filter(
            self._view_id,
            description,
            description_prefix,
            manufacturer,
            manufacturer_prefix,
            name,
            name_prefix,
            version_,
            version_prefix,
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
        property: CogniteSourceSystemFields | SequenceNotStr[CogniteSourceSystemFields] | None = None,
        query: str | None = None,
        search_property: CogniteSourceSystemTextFields | SequenceNotStr[CogniteSourceSystemTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
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
        property: CogniteSourceSystemFields | SequenceNotStr[CogniteSourceSystemFields] | None = None,
        query: str | None = None,
        search_property: CogniteSourceSystemTextFields | SequenceNotStr[CogniteSourceSystemTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
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
        group_by: CogniteSourceSystemFields | SequenceNotStr[CogniteSourceSystemFields],
        property: CogniteSourceSystemFields | SequenceNotStr[CogniteSourceSystemFields] | None = None,
        query: str | None = None,
        search_property: CogniteSourceSystemTextFields | SequenceNotStr[CogniteSourceSystemTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
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
        group_by: CogniteSourceSystemFields | SequenceNotStr[CogniteSourceSystemFields] | None = None,
        property: CogniteSourceSystemFields | SequenceNotStr[CogniteSourceSystemFields] | None = None,
        query: str | None = None,
        search_property: CogniteSourceSystemTextFields | SequenceNotStr[CogniteSourceSystemTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite source systems

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            manufacturer: The manufacturer to filter on.
            manufacturer_prefix: The prefix of the manufacturer to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            version_: The version to filter on.
            version_prefix: The prefix of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite source systems to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite source systems in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_source_system.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_source_system_filter(
            self._view_id,
            description,
            description_prefix,
            manufacturer,
            manufacturer_prefix,
            name,
            name_prefix,
            version_,
            version_prefix,
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
        property: CogniteSourceSystemFields,
        interval: float,
        query: str | None = None,
        search_property: CogniteSourceSystemTextFields | SequenceNotStr[CogniteSourceSystemTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite source systems

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            manufacturer: The manufacturer to filter on.
            manufacturer_prefix: The prefix of the manufacturer to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            version_: The version to filter on.
            version_prefix: The prefix of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite source systems to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_source_system_filter(
            self._view_id,
            description,
            description_prefix,
            manufacturer,
            manufacturer_prefix,
            name,
            name_prefix,
            version_,
            version_prefix,
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

    def select(self) -> CogniteSourceSystemQuery:
        """Start selecting from Cognite source systems."""
        return CogniteSourceSystemQuery(self._client)

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=sort,
                limit=limit,
                max_retrieve_batch_limit=chunk_size,
                has_container_fields=True,
            )
        )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[CogniteSourceSystemList]:
        """Iterate over Cognite source systems

        Args:
            chunk_size: The number of Cognite source systems to return in each iteration. Defaults to 100.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            manufacturer: The manufacturer to filter on.
            manufacturer_prefix: The prefix of the manufacturer to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            version_: The version to filter on.
            version_prefix: The prefix of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of Cognite source systems to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of Cognite source systems

        Examples:

            Iterate Cognite source systems in chunks of 100 up to 2000 items:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_source_systems in client.cognite_source_system.iterate(chunk_size=100, limit=2000):
                ...     for cognite_source_system in cognite_source_systems:
                ...         print(cognite_source_system.external_id)

            Iterate Cognite source systems in chunks of 100 sorted by external_id in descending order:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_source_systems in client.cognite_source_system.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for cognite_source_system in cognite_source_systems:
                ...         print(cognite_source_system.external_id)

            Iterate Cognite source systems in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for first_iteration in client.cognite_source_system.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for cognite_source_systems in client.cognite_source_system.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for cognite_source_system in cognite_source_systems:
                ...         print(cognite_source_system.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_cognite_source_system_filter(
            self._view_id,
            description,
            description_prefix,
            manufacturer,
            manufacturer_prefix,
            name,
            name_prefix,
            version_,
            version_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        manufacturer: str | list[str] | None = None,
        manufacturer_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteSourceSystemFields | Sequence[CogniteSourceSystemFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteSourceSystemList:
        """List/filter Cognite source systems

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            manufacturer: The manufacturer to filter on.
            manufacturer_prefix: The prefix of the manufacturer to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            version_: The version to filter on.
            version_prefix: The prefix of the version to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite source systems to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested Cognite source systems

        Examples:

            List Cognite source systems and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_source_systems = client.cognite_source_system.list(limit=5)

        """
        filter_ = _create_cognite_source_system_filter(
            self._view_id,
            description,
            description_prefix,
            manufacturer,
            manufacturer_prefix,
            name,
            name_prefix,
            version_,
            version_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
