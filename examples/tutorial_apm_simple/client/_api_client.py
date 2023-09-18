from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from ._api.asset import AssetAPI
from ._api.cdf_3_d_connection_properties import CdfConnectionPropertiesAPI
from ._api.cdf_3_d_entity import CdfEntityAPI
from ._api.cdf_3_d_model import CdfModelAPI
from ._api.work_item import WorkItemAPI
from ._api.work_order import WorkOrderAPI


class ApmSimpleClient:
    """
    ApmSimpleClient

    Generated with:
        pygen = 0.18.1
        cognite-sdk = 6.25.1
        pydantic = 2.3.0

    Data Model:
        space: tutorial_apm_simple
        externalId: ApmSimple
        version: 6
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        self.asset = AssetAPI(client)
        self.cdf_3_d_connection_properties = CdfConnectionPropertiesAPI(client)
        self.cdf_3_d_entity = CdfEntityAPI(client)
        self.cdf_3_d_model = CdfModelAPI(client)
        self.work_item = WorkItemAPI(client)
        self.work_order = WorkOrderAPI(client)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> ApmSimpleClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> ApmSimpleClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError:
                raise ValueError(f"Could not find section '{section}' in {file_path}")

        return cls.azure_project(**toml_content)
