from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core._api.cognite_unit_query import CogniteUnitQueryAPI
from cognite_core.data_classes import (
    CogniteUnit,
    CogniteUnitFields,
    CogniteUnitList,
    CogniteUnitTextFields,
    CogniteUnitWrite,
    CogniteUnitWriteList,
    ResourcesWriteResult,
)
from cognite_core.data_classes._cognite_unit import (
    _COGNITEUNIT_PROPERTIES_BY_FIELD,
    CogniteUnitQuery,
    _create_cognite_unit_filter,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
)


class CogniteUnitAPI(NodeAPI[CogniteUnit, CogniteUnitWrite, CogniteUnitList, CogniteUnitWriteList]):
    _view_id = dm.ViewId("cdf_cdm", "CogniteUnit", "v1")
    _properties_by_field = _COGNITEUNIT_PROPERTIES_BY_FIELD
    _class_type = CogniteUnit
    _class_list = CogniteUnitList
    _class_write_list = CogniteUnitWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
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
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogniteUnitQueryAPI[CogniteUnitList]:
        """Query starting at Cognite units.

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
            limit: Maximum number of Cognite units to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite units.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
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
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(CogniteUnitList)
        return CogniteUnitQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        cognite_unit: CogniteUnitWrite | Sequence[CogniteUnitWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite units.

        Args:
            cognite_unit: Cognite unit or
                sequence of Cognite units to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None.
                However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cognite_unit:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CogniteUnitWrite
                >>> client = CogniteCoreClient()
                >>> cognite_unit = CogniteUnitWrite(external_id="my_cognite_unit", ...)
                >>> result = client.cognite_unit.apply(cognite_unit)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_unit.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_unit, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite unit.

        Args:
            external_id: External id of the Cognite unit to delete.
            space: The space where all the Cognite unit are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_unit by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_unit.delete("my_cognite_unit")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_unit.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CogniteUnit | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
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
                >>> cognite_unit = client.cognite_unit.retrieve("my_cognite_unit")

        """
        return self._retrieve(external_id, space)

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
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite units matching the query.

        Examples:

           Search for 'my_cognite_unit' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_units = client.cognite_unit.search('my_cognite_unit')

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
            limit: Maximum number of Cognite units to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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

    def query(self) -> CogniteUnitQuery:
        """Start a query for Cognite units."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return CogniteUnitQuery(self._client)

    def select(self) -> CogniteUnitQuery:
        """Start selecting from Cognite units."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return CogniteUnitQuery(self._client)

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
            limit: Maximum number of Cognite units to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
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

        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )
