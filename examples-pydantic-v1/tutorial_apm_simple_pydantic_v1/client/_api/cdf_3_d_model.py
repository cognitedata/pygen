from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from tutorial_apm_simple_pydantic_v1.client.data_classes import (
    CdfModel,
    CdfModelApply,
    CdfModelList,
    CdfModelApplyList,
    CdfModelFields,
    CdfModelTextFields,
    DomainModelApply,
)
from tutorial_apm_simple_pydantic_v1.client.data_classes._cdf_3_d_model import _CDFMODEL_PROPERTIES_BY_FIELD


class CdfModelEntitiesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "cdf_3d_schema"
    ) -> dm.EdgeList:
        """Retrieve one or more entities edges by id(s) of a cdf 3 d model.

        Args:
            external_id: External id or list of external ids source cdf 3 d model.
            space: The space where all the entity edges are located.

        Returns:
            The requested entity edges.

        Examples:

            Retrieve entities edge by id:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_model = client.cdf_3_d_model.entities.retrieve("my_entities")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cdf_3d_schema", "externalId": "cdf3dEntityConnection"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_cdf_3_d_models = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_cdf_3_d_models = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_cdf_3_d_models)
        )

    def list(
        self,
        cdf_3_d_model_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "cdf_3d_schema",
    ) -> dm.EdgeList:
        """List entities edges of a cdf 3 d model.

        Args:
            cdf_3_d_model_id: ID of the source cdf 3 d model.
            limit: Maximum number of entity edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the entity edges are located.

        Returns:
            The requested entity edges.

        Examples:

            List 5 entities edges connected to "my_cdf_3_d_model":

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_model = client.cdf_3_d_model.entities.list("my_cdf_3_d_model", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "cdf_3d_schema", "externalId": "cdf3dEntityConnection"},
            )
        ]
        if cdf_3_d_model_id:
            cdf_3_d_model_ids = cdf_3_d_model_id if isinstance(cdf_3_d_model_id, list) else [cdf_3_d_model_id]
            is_cdf_3_d_models = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in cdf_3_d_model_ids
                ],
            )
            filters.append(is_cdf_3_d_models)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class CdfModelAPI(TypeAPI[CdfModel, CdfModelApply, CdfModelList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CdfModelApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CdfModel,
            class_apply_type=CdfModelApply,
            class_list=CdfModelList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.entities = CdfModelEntitiesAPI(client)

    def apply(
        self, cdf_3_d_model: CdfModelApply | Sequence[CdfModelApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) cdf 3 d models.

        Note: This method iterates through all nodes linked to cdf_3_d_model and create them including the edges
        between the nodes. For example, if any of `entities` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            cdf_3_d_model: Cdf 3 d model or sequence of cdf 3 d models to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new cdf_3_d_model:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> from tutorial_apm_simple_pydantic_v1.client.data_classes import CdfModelApply
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_model = CdfModelApply(external_id="my_cdf_3_d_model", ...)
                >>> result = client.cdf_3_d_model.apply(cdf_3_d_model)

        """
        if isinstance(cdf_3_d_model, CdfModelApply):
            instances = cdf_3_d_model.to_instances_apply(self._view_by_write_class)
        else:
            instances = CdfModelApplyList(cdf_3_d_model).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space: str = "cdf_3d_schema") -> dm.InstancesDeleteResult:
        """Delete one or more cdf 3 d model.

        Args:
            external_id: External id of the cdf 3 d model to delete.
            space: The space where all the cdf 3 d model are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cdf_3_d_model by id:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> client.cdf_3_d_model.delete("my_cdf_3_d_model")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CdfModel:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CdfModelList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "cdf_3d_schema") -> CdfModel | CdfModelList:
        """Retrieve one or more cdf 3 d models by id(s).

        Args:
            external_id: External id or list of external ids of the cdf 3 d models.
            space: The space where all the cdf 3 d models are located.

        Returns:
            The requested cdf 3 d models.

        Examples:

            Retrieve cdf_3_d_model by id:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_model = client.cdf_3_d_model.retrieve("my_cdf_3_d_model")

        """
        if isinstance(external_id, str):
            cdf_3_d_model = self._retrieve((space, external_id))

            entity_edges = self.entities.retrieve(external_id, space=space)
            cdf_3_d_model.entities = [edge.end_node.external_id for edge in entity_edges]

            return cdf_3_d_model
        else:
            cdf_3_d_models = self._retrieve([(space, ext_id) for ext_id in external_id])

            entity_edges = self.entities.retrieve(cdf_3_d_models.as_node_ids())
            self._set_entities(cdf_3_d_models, entity_edges)

            return cdf_3_d_models

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
            retrieve_edges: Whether to retrieve `entities` external ids for the cdf 3 d models. Defaults to True.

        Returns:
            Search results cdf 3 d models matching the query.

        Examples:

           Search for 'my_cdf_3_d_model' in all text properties:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_models = client.cdf_3_d_model.search('my_cdf_3_d_model')

        """
        filter_ = _create_filter(
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
            retrieve_edges: Whether to retrieve `entities` external ids for the cdf 3 d models. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count cdf 3 d models in space `my_space`:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> result = client.cdf_3_d_model.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
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
            retrieve_edges: Whether to retrieve `entities` external ids for the cdf 3 d models. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
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

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_models = client.cdf_3_d_model.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )

        cdf_3_d_models = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            space_arg = {"space": space} if space else {}
            if len(ids := cdf_3_d_models.as_node_ids()) > IN_FILTER_LIMIT:
                entity_edges = self.entities.list(limit=-1, **space_arg)
            else:
                entity_edges = self.entities.list(ids, limit=-1)
            self._set_entities(cdf_3_d_models, entity_edges)

        return cdf_3_d_models

    @staticmethod
    def _set_entities(cdf_3_d_models: Sequence[CdfModel], entity_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in entity_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for cdf_3_d_model in cdf_3_d_models:
            node_id = cdf_3_d_model.id_tuple()
            if node_id in edges_by_start_node:
                cdf_3_d_model.entities = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
