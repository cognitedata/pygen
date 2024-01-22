from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.cdf_external_references import CDFExternalReferencesAPI
from ._api.cdf_external_references_listed import CDFExternalReferencesListedAPI
from ._api.connection_item_a import ConnectionItemAAPI
from ._api.connection_item_b import ConnectionItemBAPI
from ._api.connection_item_c import ConnectionItemCAPI
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
from . import data_classes


class OmniClient:
    """
    OmniClient

    Generated with:
        pygen = 0.36.0
        cognite-sdk = 7.13.6
        pydantic = 2.5.3

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
        client.config.client_name = "CognitePygen:0.36.0"

        view_by_read_class = {
            data_classes.CDFExternalReferences: dm.ViewId("pygen-models", "CDFExternalReferences", "1"),
            data_classes.CDFExternalReferencesListed: dm.ViewId("pygen-models", "CDFExternalReferencesListed", "1"),
            data_classes.ConnectionItemA: dm.ViewId("pygen-models", "ConnectionItemA", "1"),
            data_classes.ConnectionItemB: dm.ViewId("pygen-models", "ConnectionItemB", "1"),
            data_classes.ConnectionItemC: dm.ViewId("pygen-models", "ConnectionItemC", "1"),
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

        self.cdf_external_references = CDFExternalReferencesAPI(client, view_by_read_class)
        self.cdf_external_references_listed = CDFExternalReferencesListedAPI(client, view_by_read_class)
        self.connection_item_a = ConnectionItemAAPI(client, view_by_read_class)
        self.connection_item_b = ConnectionItemBAPI(client, view_by_read_class)
        self.connection_item_c = ConnectionItemCAPI(client, view_by_read_class)
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
"""
