from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from tutorial_apm_simple_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
    CdfConnectionPropertiesFields,
    CdfConnectionPropertiesList,
    CdfConnectionPropertiesApplyList,
)
from tutorial_apm_simple_pydantic_v1.client.data_classes._cdf_3_d_connection_properties import (
    _CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD,
    _create_cdf_3_d_connection_property_filter,
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
from .cdf_3_d_connection_properties_query import CdfConnectionPropertiesQueryAPI


class CdfConnectionPropertiesAPI(
    NodeAPI[CdfConnectionProperties, CdfConnectionPropertiesApply, CdfConnectionPropertiesList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CdfConnectionPropertiesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CdfConnectionProperties,
            class_apply_type=CdfConnectionPropertiesApply,
            class_list=CdfConnectionPropertiesList,
            class_apply_list=CdfConnectionPropertiesApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CdfConnectionPropertiesQueryAPI[CdfConnectionPropertiesList]:
        """Query starting at cdf 3 d connection properties.

        Args:
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            min_revision_node_id: The minimum value of the revision node id to filter on.
            max_revision_node_id: The maximum value of the revision node id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d connection properties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for cdf 3 d connection properties.

        """
        filter_ = _create_cdf_3_d_connection_property_filter(
            self._view_id,
            min_revision_id,
            max_revision_id,
            min_revision_node_id,
            max_revision_node_id,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            CdfConnectionPropertiesList,
            [
                QueryStep(
                    name="cdf_3_d_connection_property",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_id, list(_CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD.values())
                            )
                        ]
                    ),
                    result_cls=CdfConnectionProperties,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return CdfConnectionPropertiesQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self,
        cdf_3_d_connection_property: CdfConnectionPropertiesApply | Sequence[CdfConnectionPropertiesApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) cdf 3 d connection properties.

        Args:
            cdf_3_d_connection_property: Cdf 3 d connection property or sequence of cdf 3 d connection properties to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cdf_3_d_connection_property:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> from tutorial_apm_simple_pydantic_v1.client.data_classes import CdfConnectionPropertiesApply
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_connection_property = CdfConnectionPropertiesApply(external_id="my_cdf_3_d_connection_property", ...)
                >>> result = client.cdf_3_d_connection_properties.apply(cdf_3_d_connection_property)

        """
        return self._apply(cdf_3_d_connection_property, replace)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = "cdf_3d_schema") -> dm.InstancesDeleteResult:
        """Delete one or more cdf 3 d connection property.

        Args:
            external_id: External id of the cdf 3 d connection property to delete.
            space: The space where all the cdf 3 d connection property are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cdf_3_d_connection_property by id:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> client.cdf_3_d_connection_properties.delete("my_cdf_3_d_connection_property")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> CdfConnectionProperties | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> CdfConnectionPropertiesList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "cdf_3d_schema"
    ) -> CdfConnectionProperties | CdfConnectionPropertiesList | None:
        """Retrieve one or more cdf 3 d connection properties by id(s).

        Args:
            external_id: External id or list of external ids of the cdf 3 d connection properties.
            space: The space where all the cdf 3 d connection properties are located.

        Returns:
            The requested cdf 3 d connection properties.

        Examples:

            Retrieve cdf_3_d_connection_property by id:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_connection_property = client.cdf_3_d_connection_properties.retrieve("my_cdf_3_d_connection_property")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CdfConnectionPropertiesFields | Sequence[CdfConnectionPropertiesFields] | None = None,
        group_by: None = None,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
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
        property: CdfConnectionPropertiesFields | Sequence[CdfConnectionPropertiesFields] | None = None,
        group_by: CdfConnectionPropertiesFields | Sequence[CdfConnectionPropertiesFields] = None,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
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
        property: CdfConnectionPropertiesFields | Sequence[CdfConnectionPropertiesFields] | None = None,
        group_by: CdfConnectionPropertiesFields | Sequence[CdfConnectionPropertiesFields] | None = None,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across cdf 3 d connection properties

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            min_revision_node_id: The minimum value of the revision node id to filter on.
            max_revision_node_id: The maximum value of the revision node id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d connection properties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cdf 3 d connection properties in space `my_space`:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> result = client.cdf_3_d_connection_properties.aggregate("count", space="my_space")

        """

        filter_ = _create_cdf_3_d_connection_property_filter(
            self._view_id,
            min_revision_id,
            max_revision_id,
            min_revision_node_id,
            max_revision_node_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CdfConnectionPropertiesFields,
        interval: float,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cdf 3 d connection properties

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            min_revision_node_id: The minimum value of the revision node id to filter on.
            max_revision_node_id: The maximum value of the revision node id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d connection properties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cdf_3_d_connection_property_filter(
            self._view_id,
            min_revision_id,
            max_revision_id,
            min_revision_node_id,
            max_revision_node_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CdfConnectionPropertiesList:
        """List/filter cdf 3 d connection properties

        Args:
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            min_revision_node_id: The minimum value of the revision node id to filter on.
            max_revision_node_id: The maximum value of the revision node id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d connection properties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested cdf 3 d connection properties

        Examples:

            List cdf 3 d connection properties and limit to 5:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_connection_properties = client.cdf_3_d_connection_properties.list(limit=5)

        """
        filter_ = _create_cdf_3_d_connection_property_filter(
            self._view_id,
            min_revision_id,
            max_revision_id,
            min_revision_node_id,
            max_revision_node_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
