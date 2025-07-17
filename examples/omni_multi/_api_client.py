from __future__ import annotations

import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cognite.client import ClientConfig, CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import FileMetadataList, SequenceList, TimeSeriesList

from omni_multi import data_classes
from omni_multi._api import (
    Implementation1v1API,
    Implementation1v2API,
    MainInterfaceAPI,
    SubInterfaceAPI,
)
from omni_multi._api._core import GraphQLQueryResponse, SequenceNotStr
from omni_multi.data_classes._core import GraphQLList


class OmniMultiAAPIs:
    """
    OmniMultiAAPIs

    Data Model:
        space: pygen-models
        externalId: OmniMultiA
        version: 1

    """

    _data_model_id = dm.DataModelId("pygen-models", "OmniMultiA", "1")

    def __init__(self, client: CogniteClient):
        self._client = client

        self.main_interface = MainInterfaceAPI(client)
        self.sub_interface = SubInterfaceAPI(client)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the OmniMultiA data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("pygen-models", "OmniMultiA", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class OmniMultiBAPIs:
    """
    OmniMultiBAPIs

    Data Model:
        space: pygen-models
        externalId: OmniMultiB
        version: 1

    """

    _data_model_id = dm.DataModelId("pygen-models", "OmniMultiA", "1")

    def __init__(self, client: CogniteClient):
        self._client = client

        self.implementation_1_v_2 = Implementation1v2API(client)
        self.main_interface = MainInterfaceAPI(client)
        self.sub_interface = SubInterfaceAPI(client)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the OmniMultiB data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("pygen-models", "OmniMultiB", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class OmniMultiCAPIs:
    """
    OmniMultiCAPIs

    Data Model:
        space: pygen-models
        externalId: OmniMultiC
        version: 1

    """

    _data_model_id = dm.DataModelId("pygen-models", "OmniMultiA", "1")

    def __init__(self, client: CogniteClient):
        self._client = client

        self.implementation_1_v_1 = Implementation1v1API(client)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the OmniMultiC data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("pygen-models", "OmniMultiC", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class OmniMultiClient:
    """
    OmniMultiClient

    Generated with:
        pygen = 0.0.0
        cognite-sdk = 7.75.1
        pydantic = 2.10.6

    """

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

        self.omni_multi_a = OmniMultiAAPIs(client)
        self.omni_multi_b = OmniMultiBAPIs(client)
        self.omni_multi_c = OmniMultiCAPIs(client)

        self._client = client

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
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
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
        space: str | None = None,
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

                >>> from omni_multi import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> client.delete("my_node_external_id")
        """
        if space is None and (
            isinstance(external_id, str)
            or (isinstance(external_id, Sequence) and any(isinstance(item, str) for item in external_id))
        ):
            raise ValueError("Expected space to be set when deleting by external_id")
        if isinstance(external_id, str) and space is not None:
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
                if isinstance(item, str) and space is not None:
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

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> OmniMultiClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> OmniMultiClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
