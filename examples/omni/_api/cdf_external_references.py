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
from omni.data_classes._cdf_external_references import (
    CDFExternalReferencesQuery,
    _CDFEXTERNALREFERENCES_PROPERTIES_BY_FIELD,
    _create_cdf_external_reference_filter,
)
from omni.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CDFExternalReferences,
    CDFExternalReferencesWrite,
    CDFExternalReferencesFields,
    CDFExternalReferencesList,
    CDFExternalReferencesWriteList,
    CDFExternalReferencesTextFields,
)
from omni._api.cdf_external_references_timeseries import CDFExternalReferencesTimeseriesAPI
from omni._api.cdf_external_references_query import CDFExternalReferencesQueryAPI


class CDFExternalReferencesAPI(
    NodeAPI[
        CDFExternalReferences, CDFExternalReferencesWrite, CDFExternalReferencesList, CDFExternalReferencesWriteList
    ]
):
    _view_id = dm.ViewId("sp_pygen_models", "CDFExternalReferences", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _CDFEXTERNALREFERENCES_PROPERTIES_BY_FIELD
    _class_type = CDFExternalReferences
    _class_list = CDFExternalReferencesList
    _class_write_list = CDFExternalReferencesWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.timeseries = CDFExternalReferencesTimeseriesAPI(client, self._view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CDFExternalReferencesQueryAPI[CDFExternalReferences, CDFExternalReferencesList]:
        """Query starting at cdf external references.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for cdf external references.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cdf_external_reference_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return CDFExternalReferencesQueryAPI(
            self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit
        )

    def apply(
        self,
        cdf_external_reference: CDFExternalReferencesWrite | Sequence[CDFExternalReferencesWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) cdf external references.

        Args:
            cdf_external_reference: Cdf external reference or
                sequence of cdf external references to upsert.
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

            Create a new cdf_external_reference:

                >>> from omni import OmniClient
                >>> from omni.data_classes import CDFExternalReferencesWrite
                >>> client = OmniClient()
                >>> cdf_external_reference = CDFExternalReferencesWrite(
                ...     external_id="my_cdf_external_reference", ...
                ... )
                >>> result = client.cdf_external_references.apply(cdf_external_reference)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cdf_external_references.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cdf_external_reference, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more cdf external reference.

        Args:
            external_id: External id of the cdf external reference to delete.
            space: The space where all the cdf external reference are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cdf_external_reference by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.cdf_external_references.delete("my_cdf_external_reference")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cdf_external_references.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CDFExternalReferences | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CDFExternalReferencesList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CDFExternalReferences | CDFExternalReferencesList | None:
        """Retrieve one or more cdf external references by id(s).

        Args:
            external_id: External id or list of external ids of the cdf external references.
            space: The space where all the cdf external references are located.

        Returns:
            The requested cdf external references.

        Examples:

            Retrieve cdf_external_reference by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_reference = client.cdf_external_references.retrieve(
                ...     "my_cdf_external_reference"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: CDFExternalReferencesTextFields | SequenceNotStr[CDFExternalReferencesTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CDFExternalReferencesFields | SequenceNotStr[CDFExternalReferencesFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CDFExternalReferencesList:
        """Search cdf external references

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results cdf external references matching the query.

        Examples:

           Search for 'my_cdf_external_reference' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_references = client.cdf_external_references.search(
                ...     'my_cdf_external_reference'
                ... )

        """
        filter_ = _create_cdf_external_reference_filter(
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
        property: CDFExternalReferencesFields | SequenceNotStr[CDFExternalReferencesFields] | None = None,
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
        property: CDFExternalReferencesFields | SequenceNotStr[CDFExternalReferencesFields] | None = None,
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
        group_by: CDFExternalReferencesFields | SequenceNotStr[CDFExternalReferencesFields],
        property: CDFExternalReferencesFields | SequenceNotStr[CDFExternalReferencesFields] | None = None,
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
        group_by: CDFExternalReferencesFields | SequenceNotStr[CDFExternalReferencesFields] | None = None,
        property: CDFExternalReferencesFields | SequenceNotStr[CDFExternalReferencesFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across cdf external references

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cdf external references in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.cdf_external_references.aggregate("count", space="my_space")

        """

        filter_ = _create_cdf_external_reference_filter(
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
        property: CDFExternalReferencesFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cdf external references

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cdf_external_reference_filter(
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

    def select(self) -> CDFExternalReferencesQuery:
        """Start selecting from cdf external references."""
        return CDFExternalReferencesQuery(self._client)

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
        sort_by: CDFExternalReferencesFields | Sequence[CDFExternalReferencesFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CDFExternalReferencesList:
        """List/filter cdf external references

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf external references to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested cdf external references

        Examples:

            List cdf external references and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> cdf_external_references = client.cdf_external_references.list(limit=5)

        """
        filter_ = _create_cdf_external_reference_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
