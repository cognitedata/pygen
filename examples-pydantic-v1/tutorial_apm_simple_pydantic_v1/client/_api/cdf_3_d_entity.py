from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from tutorial_apm_simple_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from tutorial_apm_simple_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    CdfEntity,
    CdfEntityApply,
    CdfEntityList,
    CdfEntityApplyList,
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
    CdfConnectionPropertiesList,
)
from tutorial_apm_simple_pydantic_v1.client.data_classes._cdf_3_d_entity import (
    _create_cdf_3_d_entity_filter,
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
from .cdf_3_d_entity_in_model_3_d import CdfEntityInModelDAPI
from .cdf_3_d_entity_query import CdfEntityQueryAPI


class CdfEntityAPI(NodeAPI[CdfEntity, CdfEntityApply, CdfEntityList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CdfEntityApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CdfEntity,
            class_apply_type=CdfEntityApply,
            class_list=CdfEntityList,
            class_apply_list=CdfEntityApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.in_model_3_d_edge = CdfEntityInModelDAPI(
            client,
            view_by_write_class,
            CdfConnectionProperties,
            CdfConnectionPropertiesApply,
            CdfConnectionPropertiesList,
        )

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CdfEntityQueryAPI[CdfEntityList]:
        """Query starting at cdf 3 d entities.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d entities to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for cdf 3 d entities.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cdf_3_d_entity_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(CdfEntityList)
        return CdfEntityQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, cdf_3_d_entity: CdfEntityApply | Sequence[CdfEntityApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) cdf 3 d entities.

        Note: This method iterates through all nodes and timeseries linked to cdf_3_d_entity and creates them including the edges
        between the nodes. For example, if any of `in_model_3_d` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            cdf_3_d_entity: Cdf 3 d entity or sequence of cdf 3 d entities to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cdf_3_d_entity:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> from tutorial_apm_simple_pydantic_v1.client.data_classes import CdfEntityApply
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_entity = CdfEntityApply(external_id="my_cdf_3_d_entity", ...)
                >>> result = client.cdf_3_d_entity.apply(cdf_3_d_entity)

        """
        return self._apply(cdf_3_d_entity, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more cdf 3 d entity.

        Args:
            external_id: External id of the cdf 3 d entity to delete.
            space: The space where all the cdf 3 d entity are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cdf_3_d_entity by id:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> client.cdf_3_d_entity.delete("my_cdf_3_d_entity")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> CdfEntity | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> CdfEntityList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CdfEntity | CdfEntityList | None:
        """Retrieve one or more cdf 3 d entities by id(s).

        Args:
            external_id: External id or list of external ids of the cdf 3 d entities.
            space: The space where all the cdf 3 d entities are located.

        Returns:
            The requested cdf 3 d entities.

        Examples:

            Retrieve cdf_3_d_entity by id:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_entity = client.cdf_3_d_entity.retrieve("my_cdf_3_d_entity")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_triple=[
                (
                    self.in_model_3_d_edge,
                    "in_model_3_d",
                    dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
                ),
            ],
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> CdfEntityList:
        """List/filter cdf 3 d entities

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cdf 3 d entities to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `in_model_3_d` external ids for the cdf 3 d entities. Defaults to True.

        Returns:
            List of requested cdf 3 d entities

        Examples:

            List cdf 3 d entities and limit to 5:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_entities = client.cdf_3_d_entity.list(limit=5)

        """
        filter_ = _create_cdf_3_d_entity_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_triple=[
                (
                    self.in_model_3_d_edge,
                    "in_model_3_d",
                    dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
                ),
            ],
        )
