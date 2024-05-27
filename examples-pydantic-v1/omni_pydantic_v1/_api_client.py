from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any, Sequence

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.credentials import OAuthClientCredentials

from ._api.cdf_external_references import CDFExternalReferencesAPI
from ._api.cdf_external_references_listed import CDFExternalReferencesListedAPI
from ._api.connection_item_a import ConnectionItemAAPI
from ._api.connection_item_b import ConnectionItemBAPI
from ._api.connection_item_c import ConnectionItemCAPI
from ._api.connection_item_d import ConnectionItemDAPI
from ._api.connection_item_e import ConnectionItemEAPI
from ._api.connection_item_f import ConnectionItemFAPI
from ._api.connection_item_g import ConnectionItemGAPI
from ._api.dependent_on_non_writable import DependentOnNonWritableAPI
from ._api.empty import EmptyAPI
from ._api.implementation_1 import Implementation1API
from ._api.implementation_1_non_writeable import Implementation1NonWriteableAPI
from ._api.implementation_2 import Implementation2API
from ._api.main_interface import MainInterfaceAPI
from ._api.primitive_nullable import PrimitiveNullableAPI
from ._api.primitive_nullable_listed import PrimitiveNullableListedAPI
from ._api.primitive_required import PrimitiveRequiredAPI
from ._api.primitive_required_listed import PrimitiveRequiredListedAPI
from ._api.primitive_with_defaults import PrimitiveWithDefaultsAPI
from ._api.sub_interface import SubInterfaceAPI
from ._api._core import SequenceNotStr, GraphQLQueryResponse
from .data_classes._core import DEFAULT_INSTANCE_SPACE, GraphQLList
from . import data_classes


class OmniClient:
    """
    OmniClient

    Generated with:
        pygen = 0.99.23
        cognite-sdk = 7.43.5
        pydantic = 1.10.7

    Data Model:
        space: pygen-models
        externalId: Omni
        version: 1
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        # The client name is used for aggregated logging of Pygen Usage
        client.config.client_name = "CognitePygen:0.99.23"

        view_by_read_class = {
            data_classes.CDFExternalReferences: dm.ViewId("pygen-models", "CDFExternalReferences", "1"),
            data_classes.CDFExternalReferencesListed: dm.ViewId("pygen-models", "CDFExternalReferencesListed", "1"),
            data_classes.ConnectionEdgeA: dm.ViewId("pygen-models", "ConnectionEdgeA", "1"),
            data_classes.ConnectionItemA: dm.ViewId("pygen-models", "ConnectionItemA", "1"),
            data_classes.ConnectionItemB: dm.ViewId("pygen-models", "ConnectionItemB", "1"),
            data_classes.ConnectionItemC: dm.ViewId("pygen-models", "ConnectionItemC", "1"),
            data_classes.ConnectionItemD: dm.ViewId("pygen-models", "ConnectionItemD", "1"),
            data_classes.ConnectionItemE: dm.ViewId("pygen-models", "ConnectionItemE", "1"),
            data_classes.ConnectionItemF: dm.ViewId("pygen-models", "ConnectionItemF", "1"),
            data_classes.ConnectionItemG: dm.ViewId("pygen-models", "ConnectionItemG", "1"),
            data_classes.DependentOnNonWritable: dm.ViewId("pygen-models", "DependentOnNonWritable", "1"),
            data_classes.Empty: dm.ViewId("pygen-models", "Empty", "1"),
            data_classes.Implementation1: dm.ViewId("pygen-models", "Implementation1", "1"),
            data_classes.Implementation1NonWriteable: dm.ViewId("pygen-models", "Implementation1NonWriteable", "1"),
            data_classes.Implementation2: dm.ViewId("pygen-models", "Implementation2", "1"),
            data_classes.MainInterface: dm.ViewId("pygen-models", "MainInterface", "1"),
            data_classes.PrimitiveNullable: dm.ViewId("pygen-models", "PrimitiveNullable", "1"),
            data_classes.PrimitiveNullableListed: dm.ViewId("pygen-models", "PrimitiveNullableListed", "1"),
            data_classes.PrimitiveRequired: dm.ViewId("pygen-models", "PrimitiveRequired", "1"),
            data_classes.PrimitiveRequiredListed: dm.ViewId("pygen-models", "PrimitiveRequiredListed", "1"),
            data_classes.PrimitiveWithDefaults: dm.ViewId("pygen-models", "PrimitiveWithDefaults", "1"),
            data_classes.SubInterface: dm.ViewId("pygen-models", "SubInterface", "1"),
        }
        self._view_by_read_class = view_by_read_class
        self._client = client

        self.cdf_external_references = CDFExternalReferencesAPI(client, view_by_read_class)
        self.cdf_external_references_listed = CDFExternalReferencesListedAPI(client, view_by_read_class)
        self.connection_item_a = ConnectionItemAAPI(client, view_by_read_class)
        self.connection_item_b = ConnectionItemBAPI(client, view_by_read_class)
        self.connection_item_c = ConnectionItemCAPI(client, view_by_read_class)
        self.connection_item_d = ConnectionItemDAPI(client, view_by_read_class)
        self.connection_item_e = ConnectionItemEAPI(client, view_by_read_class)
        self.connection_item_f = ConnectionItemFAPI(client, view_by_read_class)
        self.connection_item_g = ConnectionItemGAPI(client, view_by_read_class)
        self.dependent_on_non_writable = DependentOnNonWritableAPI(client, view_by_read_class)
        self.empty = EmptyAPI(client, view_by_read_class)
        self.implementation_1 = Implementation1API(client, view_by_read_class)
        self.implementation_1_non_writeable = Implementation1NonWriteableAPI(client, view_by_read_class)
        self.implementation_2 = Implementation2API(client, view_by_read_class)
        self.main_interface = MainInterfaceAPI(client, view_by_read_class)
        self.primitive_nullable = PrimitiveNullableAPI(client, view_by_read_class)
        self.primitive_nullable_listed = PrimitiveNullableListedAPI(client, view_by_read_class)
        self.primitive_required = PrimitiveRequiredAPI(client, view_by_read_class)
        self.primitive_required_listed = PrimitiveRequiredListedAPI(client, view_by_read_class)
        self.primitive_with_defaults = PrimitiveWithDefaultsAPI(client, view_by_read_class)
        self.sub_interface = SubInterfaceAPI(client, view_by_read_class)

    def upsert(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        replace: bool = False,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> data_classes.ResourcesWriteResult:
        """Add or update (upsert) items.

        Args:
            items: One or more instances of the pygen generated data classes.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method will, by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
            allow_version_increase (bool): If set to true, the version of the instance will be increased if the instance already exists.
                If you get an error: 'A version conflict caused the ingest to fail', you can set this to true to allow
                the version to increase.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        """
        instances = self._create_instances(items, write_none, allow_version_increase)
        result = self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )
        time_series = []
        if instances.time_series:
            time_series = self._client.time_series.upsert(instances.time_series, mode="patch")

        return data_classes.ResourcesWriteResult(result.nodes, result.edges, TimeSeriesList(time_series))

    def _create_instances(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        write_none: bool,
        allow_version_increase: bool,
    ) -> data_classes.ResourcesWrite:
        if isinstance(items, data_classes.DomainModelWrite):
            instances = items.to_instances_write(self._view_by_read_class, write_none, allow_version_increase)
        else:
            instances = data_classes.ResourcesWrite()
            cache: set[tuple[str, str]] = set()
            for item in items:
                instances.extend(
                    item._to_instances_write(
                        cache,
                        self._view_by_read_class,
                        write_none,
                        allow_version_increase,
                    )
                )
        return instances

    def apply(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> data_classes.ResourcesWriteResult:
        """Add or update (upsert) items.

        Args:
            items: One or more instances of the pygen generated data classes.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method will, by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the instead."
            "The motivation is that .upsert is a more descriptive name for the operation.",
            UserWarning,
            stacklevel=2,
        )
        return self.upsert(items, replace, write_none)

    def delete(
        self,
        external_id: (
            str | SequenceNotStr[str] | data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite]
        ),
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> dm.InstancesDeleteResult:
        """Delete one or more items.

        If you pass in an item, it will be deleted recursively, i.e., all connected nodes and edges
        will be deleted as well.

        Args:
            external_id: The external id or items(s) to delete.
            space: The space where all the item(s) are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete item by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> client.delete("my_node_external_id")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        elif all(isinstance(item, str) for item in external_id):
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )
        elif isinstance(external_id, data_classes.DomainModelWrite) or all(
            isinstance(item, data_classes.DomainModelWrite) for item in external_id
        ):
            resources = self._create_instances(external_id, False, False)
            return self._client.data_modeling.instances.delete(
                nodes=resources.nodes.as_ids(),
                edges=resources.edges.as_ids(),
            )
        else:
            raise ValueError(
                "Expected str, list of str, or DomainModelWrite, list of DomainModelWrite," f"got {type(external_id)}"
            )

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the Omni data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("pygen-models", "Omni", "1")
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
        return """<strong>OmniClient</strong> generated from data model ("pygen-models", "Omni", "1")<br />
with the following APIs available<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cdf_external_references<br />
&nbsp;&nbsp;&nbsp;&nbsp;.cdf_external_references_listed<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_a<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_b<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_c<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_d<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_e<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_f<br />
&nbsp;&nbsp;&nbsp;&nbsp;.connection_item_g<br />
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
