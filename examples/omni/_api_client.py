from __future__ import annotations

import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cognite.client import ClientConfig, CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import FileMetadataList, SequenceList, TimeSeriesList

from omni import data_classes
from omni._api import (
    CDFExternalReferencesAPI,
    CDFExternalReferencesListedAPI,
    ConnectionItemAAPI,
    ConnectionItemBAPI,
    ConnectionItemCNodeAPI,
    ConnectionItemDAPI,
    ConnectionItemEAPI,
    ConnectionItemFAPI,
    ConnectionItemGAPI,
    ConnectionItemHAPI,
    DependentOnNonWritableAPI,
    EmptyAPI,
    Implementation1API,
    Implementation1NonWriteableAPI,
    Implementation2API,
    MainInterfaceAPI,
    PrimitiveNullableAPI,
    PrimitiveNullableListedAPI,
    PrimitiveRequiredAPI,
    PrimitiveRequiredListedAPI,
    PrimitiveWithDefaultsAPI,
    SubInterfaceAPI,
)
from omni._api._core import GraphQLQueryResponse, SequenceNotStr
from omni.data_classes._core import DEFAULT_INSTANCE_SPACE, GraphQLList


class OmniClient:
    """
    OmniClient

    Generated with:
        pygen = 0.0.0
        cognite-sdk = 7.77.0
        pydantic = 2.11.7

    Data Model:
        space: sp_pygen_models
        externalId: Omni
        version: 1
    """

    _data_model_id = dm.DataModelId("sp_pygen_models", "Omni", "1")

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

        self.cdf_external_references = CDFExternalReferencesAPI(client)
        self.cdf_external_references_listed = CDFExternalReferencesListedAPI(client)
        self.connection_item_a = ConnectionItemAAPI(client)
        self.connection_item_b = ConnectionItemBAPI(client)
        self.connection_item_c_node = ConnectionItemCNodeAPI(client)
        self.connection_item_d = ConnectionItemDAPI(client)
        self.connection_item_e = ConnectionItemEAPI(client)
        self.connection_item_f = ConnectionItemFAPI(client)
        self.connection_item_g = ConnectionItemGAPI(client)
        self.connection_item_h = ConnectionItemHAPI(client)
        self.dependent_on_non_writable = DependentOnNonWritableAPI(client)
        self.empty = EmptyAPI(client)
        self.implementation_1 = Implementation1API(client)
        self.implementation_1_non_writeable = Implementation1NonWriteableAPI(client)
        self.implementation_2 = Implementation2API(client)
        self.main_interface = MainInterfaceAPI(client)
        self.primitive_nullable = PrimitiveNullableAPI(client)
        self.primitive_nullable_listed = PrimitiveNullableListedAPI(client)
        self.primitive_required = PrimitiveRequiredAPI(client)
        self.primitive_required_listed = PrimitiveRequiredListedAPI(client)
        self.primitive_with_defaults = PrimitiveWithDefaultsAPI(client)
        self.sub_interface = SubInterfaceAPI(client)

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

                >>> from omni import OmniClient
                >>> client = OmniClient()
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
        """Execute a GraphQl query against the Omni data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("sp_pygen_models", "Omni", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> OmniClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> OmniClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)

    def _repr_html_(self) -> str:
        return """<strong>OmniClient(</strong> generated from data model
("sp_pygen_models", "Omni", "1")<br />
with the following APIs available<br />
)&nbsp;&nbsp;&nbsp;&nbsp;.cdf_external_references<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cdf_external_references_listed<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_a<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_b<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_c_node<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_d<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_e<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_f<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_g<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_h<br />
&nbsp;&nbsp;&nbsp;&nbsp;.dependent_on_non_writable<br />
&nbsp;&nbsp;&nbsp;&nbsp;.empty<br />
&nbsp;&nbsp;&nbsp;&nbsp;.implementation_1<br />
&nbsp;&nbsp;&nbsp;&nbsp;.implementation_1_non_writeable<br />
&nbsp;&nbsp;&nbsp;&nbsp;.implementation_2<br />
&nbsp;&nbsp;&nbsp;&nbsp;.main_interface<br />
&nbsp;&nbsp;&nbsp;&nbsp;.primitive_nullable<br />
&nbsp;&nbsp;&nbsp;&nbsp;.primitive_nullable_listed<br />
&nbsp;&nbsp;&nbsp;&nbsp;.primitive_required<br />
&nbsp;&nbsp;&nbsp;&nbsp;.primitive_required_listed<br />
&nbsp;&nbsp;&nbsp;&nbsp;.primitive_with_defaults<br />
&nbsp;&nbsp;&nbsp;&nbsp;.sub_interface<br />
<br />
and with the methods:<br />
&nbsp;&nbsp;&nbsp;&nbsp;.upsert - Create or update any instance.<br />
&nbsp;&nbsp;&nbsp;&nbsp;.delete - Delete instances.<br />
"""
