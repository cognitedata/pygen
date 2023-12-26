from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.cdf_external_references import CDFExternalReferencesAPI
from ._api.cdf_external_references_listable import CDFExternalReferencesListableAPI
from ._api.implementation_1 import Implementation1API
from ._api.implementation_1_non_writeable import Implementation1NonWriteableAPI
from ._api.implementation_2 import Implementation2API
from ._api.main_interface import MainInterfaceAPI
from ._api.primitive_nullable import PrimitiveNullableAPI
from ._api.primitive_nullable_listable import PrimitiveNullableListableAPI
from ._api.primitive_required import PrimitiveRequiredAPI
from ._api.primitive_required_listable import PrimitiveRequiredListableAPI
from ._api.primitive_with_defaults import PrimitiveWithDefaultsAPI
from ._api.sub_interface import SubInterfaceAPI
from . import data_classes


class OmniClient:
    """
    OmniClient

    Generated with:
        pygen = 0.32.5
        cognite-sdk = 7.8.5
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
        client.config.client_name = "CognitePygen:0.32.5"

        view_by_write_class = {
            data_classes.CDFExternalReferencesApply: dm.ViewId("pygen-models", "CDFExternalReferences", "1"),
            data_classes.CDFExternalReferencesListableApply: dm.ViewId(
                "pygen-models", "CDFExternalReferencesListable", "1"
            ),
            data_classes.Implementation1Apply: dm.ViewId("pygen-models", "Implementation1", "1"),
            data_classes.Implementation1NonWriteableApply: dm.ViewId(
                "pygen-models", "Implementation1NonWriteable", "1"
            ),
            data_classes.Implementation2Apply: dm.ViewId("pygen-models", "Implementation2", "1"),
            data_classes.MainInterfaceApply: dm.ViewId("pygen-models", "MainInterface", "1"),
            data_classes.PrimitiveNullableApply: dm.ViewId("pygen-models", "PrimitiveNullable", "1"),
            data_classes.PrimitiveNullableListableApply: dm.ViewId("pygen-models", "PrimitiveNullableListable", "1"),
            data_classes.PrimitiveRequiredApply: dm.ViewId("pygen-models", "PrimitiveRequired", "1"),
            data_classes.PrimitiveRequiredListableApply: dm.ViewId("pygen-models", "PrimitiveRequiredListable", "1"),
            data_classes.PrimitiveWithDefaultsApply: dm.ViewId("pygen-models", "PrimitiveWithDefaults", "1"),
            data_classes.SubInterfaceApply: dm.ViewId("pygen-models", "SubInterface", "1"),
        }

        self.cdf_external_references = CDFExternalReferencesAPI(client, view_by_write_class)
        self.cdf_external_references_listable = CDFExternalReferencesListableAPI(client, view_by_write_class)
        self.implementation_1 = Implementation1API(client, view_by_write_class)
        self.implementation_1_non_writeable = Implementation1NonWriteableAPI(client, view_by_write_class)
        self.implementation_2 = Implementation2API(client, view_by_write_class)
        self.main_interface = MainInterfaceAPI(client, view_by_write_class)
        self.primitive_nullable = PrimitiveNullableAPI(client, view_by_write_class)
        self.primitive_nullable_listable = PrimitiveNullableListableAPI(client, view_by_write_class)
        self.primitive_required = PrimitiveRequiredAPI(client, view_by_write_class)
        self.primitive_required_listable = PrimitiveRequiredListableAPI(client, view_by_write_class)
        self.primitive_with_defaults = PrimitiveWithDefaultsAPI(client, view_by_write_class)
        self.sub_interface = SubInterfaceAPI(client, view_by_write_class)

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
