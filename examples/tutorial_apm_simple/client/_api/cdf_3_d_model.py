from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from tutorial_apm_simple.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    CdfModel,
    CdfModelApply,
    CdfModelFields,
    CdfModelList,
    CdfModelApplyList,
    CdfModelTextFields,
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
    CdfConnectionPropertiesList,
)
from tutorial_apm_simple.client.data_classes._cdf_3_d_model import (
    _CDFMODEL_PROPERTIES_BY_FIELD,
    _create_cdf_3_d_model_filter,
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
from .cdf_3_d_model_entities import CdfModelEntitiesAPI
from .cdf_3_d_model_query import CdfModelQueryAPI


class CdfModelAPI(NodeAPI[CdfModel, CdfModelApply, CdfModelList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CdfModelApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CdfModel,
            class_apply_type=CdfModelApply,
            class_list=CdfModelList,
            class_apply_list=CdfModelApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.entities_edge = CdfModelEntitiesAPI(
            client,
            view_by_write_class,
            CdfConnectionProperties,
            CdfConnectionPropertiesApply,
            CdfConnectionPropertiesList,
        )

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CdfModelQueryAPI[CdfModelList]:
        """Query starting at cdf 3 d models.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for cdf 3 d models.

        """
        filter_ = _create_cdf_3_d_model_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            CdfModelList,
            [
                QueryStep(
                    name="cdf_3_d_model",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_CDFMODEL_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=CdfModel,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return CdfModelQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, cdf_3_d_model: CdfModelApply | Sequence[CdfModelApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) cdf 3 d models.

        Note: This method iterates through all nodes and timeseries linked to cdf_3_d_model and creates them including the edges
        between the nodes. For example, if any of `entities` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            cdf_3_d_model: Cdf 3 d model or sequence of cdf 3 d models to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cdf_3_d_model:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> from tutorial_apm_simple.client.data_classes import CdfModelApply
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_model = CdfModelApply(external_id="my_cdf_3_d_model", ...)
                >>> result = client.cdf_3_d_model.apply(cdf_3_d_model)

        """
        return self._apply(cdf_3_d_model, replace)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = "cdf_3d_schema") -> dm.InstancesDeleteResult:
        """Delete one or more cdf 3 d model.

        Args:
            external_id: External id of the cdf 3 d model to delete.
            space: The space where all the cdf 3 d model are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cdf_3_d_model by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> client.cdf_3_d_model.delete("my_cdf_3_d_model")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> CdfModel:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> CdfModelList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = "cdf_3d_schema") -> CdfModel | CdfModelList:
        """Retrieve one or more cdf 3 d models by id(s).

        Args:
            external_id: External id or list of external ids of the cdf 3 d models.
            space: The space where all the cdf 3 d models are located.

        Returns:
            The requested cdf 3 d models.

        Examples:

            Retrieve cdf_3_d_model by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_model = client.cdf_3_d_model.retrieve("my_cdf_3_d_model")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_triple=[
                (self.entities_edge, "entities", dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection")),
            ],
        )

    def search(
        self,
        query: str,
        properties: CdfModelTextFields | Sequence[CdfModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CdfModelList:
        """Search cdf 3 d models

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results cdf 3 d models matching the query.

        Examples:

           Search for 'my_cdf_3_d_model' in all text properties:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_models = client.cdf_3_d_model.search('my_cdf_3_d_model')

        """
        filter_ = _create_cdf_3_d_model_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _CDFMODEL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CdfModelFields | Sequence[CdfModelFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: CdfModelTextFields | Sequence[CdfModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: CdfModelFields | Sequence[CdfModelFields] | None = None,
        group_by: CdfModelFields | Sequence[CdfModelFields] = None,
        query: str | None = None,
        search_properties: CdfModelTextFields | Sequence[CdfModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: CdfModelFields | Sequence[CdfModelFields] | None = None,
        group_by: CdfModelFields | Sequence[CdfModelFields] | None = None,
        query: str | None = None,
        search_property: CdfModelTextFields | Sequence[CdfModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across cdf 3 d models

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cdf 3 d models in space `my_space`:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> result = client.cdf_3_d_model.aggregate("count", space="my_space")

        """

        filter_ = _create_cdf_3_d_model_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _CDFMODEL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CdfModelFields,
        interval: float,
        query: str | None = None,
        search_property: CdfModelTextFields | Sequence[CdfModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cdf 3 d models

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cdf_3_d_model_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _CDFMODEL_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> CdfModelList:
        """List/filter cdf 3 d models

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `entities` external ids for the cdf 3 d models. Defaults to True.

        Returns:
            List of requested cdf 3 d models

        Examples:

            List cdf 3 d models and limit to 5:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_models = client.cdf_3_d_model.list(limit=5)

        """
        filter_ = _create_cdf_3_d_model_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_triple=[
                (self.entities_edge, "entities", dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection")),
            ],
        )
