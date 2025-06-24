from __future__ import annotations

import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cognite.client import ClientConfig, CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import FileMetadataList, SequenceList, TimeSeriesList

from cognite_core import data_classes
from cognite_core._api import (
    Cognite360ImageAPI,
    Cognite360ImageCollectionAPI,
    Cognite360ImageModelAPI,
    Cognite360ImageStationAPI,
    Cognite3DModelAPI,
    Cognite3DObjectAPI,
    Cognite3DRevisionAPI,
    Cognite3DTransformationNodeAPI,
    CogniteActivityAPI,
    CogniteAssetAPI,
    CogniteAssetClassAPI,
    CogniteAssetTypeAPI,
    CogniteCADModelAPI,
    CogniteCADNodeAPI,
    CogniteCADRevisionAPI,
    CogniteCubeMapAPI,
    CogniteDescribableNodeAPI,
    CogniteEquipmentAPI,
    CogniteEquipmentTypeAPI,
    CogniteFileAPI,
    CogniteFileCategoryAPI,
    CognitePointCloudModelAPI,
    CognitePointCloudRevisionAPI,
    CognitePointCloudVolumeAPI,
    CogniteSchedulableAPI,
    CogniteSourceSystemAPI,
    CogniteSourceableNodeAPI,
    CogniteTimeSeriesAPI,
    CogniteUnitAPI,
    CogniteVisualizableAPI,
)
from cognite_core._api._core import GraphQLQueryResponse, SequenceNotStr
from cognite_core.data_classes._core import DEFAULT_INSTANCE_SPACE, GraphQLList


class CogniteCoreClient:
    """
    CogniteCoreClient

    Generated with:
        pygen = 0.0.0
        cognite-sdk = 7.75.1
        pydantic = 2.10.6

    Data Model:
        space: cdf_cdm
        externalId: CogniteCore
        version: v1
    """

    _data_model_id = dm.DataModelId("cdf_cdm", "CogniteCore", "v1")

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        # The client name is used for aggregated logging of Pygen Usage
        if not client.config.client_name.startswith("CognitePygen"):
            client.config.client_name = f"CognitePygen:0.0.0:SDK:{client.config.client_name}"

        self._client = client

        self.cognite_360_image = Cognite360ImageAPI(client)
        self.cognite_360_image_collection = Cognite360ImageCollectionAPI(client)
        self.cognite_360_image_model = Cognite360ImageModelAPI(client)
        self.cognite_360_image_station = Cognite360ImageStationAPI(client)
        self.cognite_3_d_model = Cognite3DModelAPI(client)
        self.cognite_3_d_object = Cognite3DObjectAPI(client)
        self.cognite_3_d_revision = Cognite3DRevisionAPI(client)
        self.cognite_3_d_transformation_node = Cognite3DTransformationNodeAPI(client)
        self.cognite_activity = CogniteActivityAPI(client)
        self.cognite_asset = CogniteAssetAPI(client)
        self.cognite_asset_class = CogniteAssetClassAPI(client)
        self.cognite_asset_type = CogniteAssetTypeAPI(client)
        self.cognite_cad_model = CogniteCADModelAPI(client)
        self.cognite_cad_node = CogniteCADNodeAPI(client)
        self.cognite_cad_revision = CogniteCADRevisionAPI(client)
        self.cognite_cube_map = CogniteCubeMapAPI(client)
        self.cognite_describable_node = CogniteDescribableNodeAPI(client)
        self.cognite_equipment = CogniteEquipmentAPI(client)
        self.cognite_equipment_type = CogniteEquipmentTypeAPI(client)
        self.cognite_file = CogniteFileAPI(client)
        self.cognite_file_category = CogniteFileCategoryAPI(client)
        self.cognite_point_cloud_model = CognitePointCloudModelAPI(client)
        self.cognite_point_cloud_revision = CognitePointCloudRevisionAPI(client)
        self.cognite_point_cloud_volume = CognitePointCloudVolumeAPI(client)
        self.cognite_schedulable = CogniteSchedulableAPI(client)
        self.cognite_source_system = CogniteSourceSystemAPI(client)
        self.cognite_sourceable_node = CogniteSourceableNodeAPI(client)
        self.cognite_time_series = CogniteTimeSeriesAPI(client)
        self.cognite_unit = CogniteUnitAPI(client)
        self.cognite_visualizable = CogniteVisualizableAPI(client)

    def upsert(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        replace: bool = False,
        allow_version_increase: bool = False,
    ) -> data_classes.ResourcesWriteResult:
        """Add or update (upsert) items.

        This method will create the nodes, edges, timeseries, files and sequences of the supplied items.

        Args:
            items: One or more instances of the pygen generated data classes.
            replace (bool): How do we behave when a property value exists? Do we replace all
                matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            allow_version_increase (bool): If set to true, the version of the instance will be increased
                if the instance already exists.
                If you get an error: 'A version conflict caused the ingest to fail', you can set this to true to allow
                the version to increase.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        """
        instances = self._create_instances(items, allow_version_increase)
        result = self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )
        time_series = TimeSeriesList([])
        if instances.time_series:
            time_series = self._client.time_series.upsert(instances.time_series, mode="patch")
        files = FileMetadataList([])
        if instances.files:
            for file in instances.files:
                created, _ = self._client.files.create(file, overwrite=True)
                files.append(created)

        sequences = SequenceList([])
        if instances.sequences:
            sequences = self._client.sequences.upsert(instances.sequences, mode="patch")

        return data_classes.ResourcesWriteResult(result.nodes, result.edges, time_series, files, sequences)

    def _create_instances(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        allow_version_increase: bool,
    ) -> data_classes.ResourcesWrite:
        if isinstance(items, data_classes.DomainModelWrite):
            instances = items.to_instances_write(allow_version_increase)
        else:
            instances = data_classes.ResourcesWrite()
            cache: set[tuple[str, str]] = set()
            for item in items:
                instances.extend(
                    item._to_resources_write(
                        cache,
                        allow_version_increase,
                    )
                )
        return instances

    def delete(
        self,
        external_id: (
            str
            | dm.NodeId
            | data_classes.DomainModelWrite
            | SequenceNotStr[str | dm.NodeId | data_classes.DomainModelWrite]
        ),
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> dm.InstancesDeleteResult:
        """Delete one or more items.

        If you pass in an item, it will be deleted recursively, i.e., all connected nodes and edges
        will be deleted as well.

        Args:
            external_id: The external id or items(s) to delete. Can also be a list of NodeId(s) or DomainModelWrite(s).
            space: The space where all the item(s) are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete item by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.delete("my_node_external_id")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        elif isinstance(external_id, dm.NodeId):
            return self._client.data_modeling.instances.delete(nodes=external_id)
        elif isinstance(external_id, data_classes.DomainModelWrite):
            resources = self._create_instances(external_id, False)
            return self._client.data_modeling.instances.delete(
                nodes=resources.nodes.as_ids(),
                edges=resources.edges.as_ids(),
            )
        elif isinstance(external_id, Sequence):
            node_ids: list[dm.NodeId] = []
            edge_ids: list[dm.EdgeId] = []
            for item in external_id:
                if isinstance(item, str):
                    node_ids.append(dm.NodeId(space, item))
                elif isinstance(item, dm.NodeId):
                    node_ids.append(item)
                elif isinstance(item, data_classes.DomainModelWrite):
                    resources = self._create_instances(item, False)
                    node_ids.extend(resources.nodes.as_ids())
                    edge_ids.extend(resources.edges.as_ids())
                else:
                    raise ValueError(
                        f"Expected str, NodeId, or DomainModelWrite, Sequence of these types. Got {type(external_id)}"
                    )
            return self._client.data_modeling.instances.delete(nodes=node_ids, edges=edge_ids)
        else:
            raise ValueError(
                f"Expected str, NodeId, or DomainModelWrite, Sequence of these types. Got {type(external_id)}"
            )

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the CogniteCore data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("cdf_cdm", "CogniteCore", "v1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> CogniteCoreClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> CogniteCoreClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)

    def _repr_html_(self) -> str:
        return """<strong>CogniteCoreClient(</strong> generated from data model
("cdf_cdm", "CogniteCore", "v1")<br />
with the following APIs available<br />
)&nbsp;&nbsp;&nbsp;&nbsp;.cognite_360_image<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_360_image_collection<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_360_image_model<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_360_image_station<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_3_d_model<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_3_d_object<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_3_d_revision<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_3_d_transformation_node<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_activity<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_asset<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_asset_class<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_asset_type<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_cad_model<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_cad_node<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_cad_revision<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_cube_map<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_describable_node<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_equipment<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_equipment_type<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_file<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_file_category<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_point_cloud_model<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_point_cloud_revision<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_point_cloud_volume<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_schedulable<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_source_system<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_sourceable_node<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_time_series<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_unit<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cognite_visualizable<br />
<br />
and with the methods:<br />
&nbsp;&nbsp;&nbsp;&nbsp;.upsert - Create or update any instance.<br />
&nbsp;&nbsp;&nbsp;&nbsp;.delete - Delete instances.<br />
"""
