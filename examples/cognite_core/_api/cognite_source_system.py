from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from cognite_core.data_classes import (
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
from cognite_core.data_classes._cognite_source_system import (
    CogniteSourceSystemQuery,
    _COGNITESOURCESYSTEM_PROPERTIES_BY_FIELD,
    _create_cognite_source_system_filter,
)
from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core._api.cognite_source_system_query import CogniteSourceSystemQueryAPI


class CogniteSourceSystemAPI(
    NodeAPI[CogniteSourceSystem, CogniteSourceSystemWrite, CogniteSourceSystemList, CogniteSourceSystemWriteList]
):
    _view_id = dm.ViewId("cdf_cdm", "CogniteSourceSystem", "v1")
    _properties_by_field = _COGNITESOURCESYSTEM_PROPERTIES_BY_FIELD
    _class_type = CogniteSourceSystem
    _class_list = CogniteSourceSystemList
    _class_write_list = CogniteSourceSystemWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
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
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogniteSourceSystemQueryAPI[CogniteSourceSystemList]:
        """Query starting at Cognite source systems.

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
            limit: Maximum number of Cognite source systems to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite source systems.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
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
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(CogniteSourceSystemList)
        return CogniteSourceSystemQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        cognite_source_system: CogniteSourceSystemWrite | Sequence[CogniteSourceSystemWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite source systems.

        Args:
            cognite_source_system: Cognite source system or sequence of Cognite source systems to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cognite_source_system:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CogniteSourceSystemWrite
                >>> client = CogniteCoreClient()
                >>> cognite_source_system = CogniteSourceSystemWrite(external_id="my_cognite_source_system", ...)
                >>> result = client.cognite_source_system.apply(cognite_source_system)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_source_system.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_source_system, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite source system.

        Args:
            external_id: External id of the Cognite source system to delete.
            space: The space where all the Cognite source system are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_source_system by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_source_system.delete("my_cognite_source_system")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_source_system.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CogniteSourceSystem | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
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
                >>> cognite_source_system = client.cognite_source_system.retrieve("my_cognite_source_system")

        """
        return self._retrieve(external_id, space)

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
            limit: Maximum number of Cognite source systems to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite source systems matching the query.

        Examples:

           Search for 'my_cognite_source_system' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_source_systems = client.cognite_source_system.search('my_cognite_source_system')

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
            limit: Maximum number of Cognite source systems to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            limit: Maximum number of Cognite source systems to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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

    def query(self) -> CogniteSourceSystemQuery:
        """Start a query for Cognite source systems."""
        warnings.warn("The .query is in alpha and is subject to breaking changes without notice.")
        return CogniteSourceSystemQuery(self._client)

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
            limit: Maximum number of Cognite source systems to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
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

        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )
