from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.case import CaseAPI
from ._api.command_config import CommandConfigAPI


class ShopClient:
    """
    ShopClient

    Generated with:
        pygen = 0.22.0
        cognite-sdk = 6.25.3
        pydantic = 1.10.7

    Data Model:
        space: IntegrationTestsImmutable
        externalId: SHOP_Model
        version: 2
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        self.case = CaseAPI(client, dm.ViewId("IntegrationTestsImmutable", "Case", "366b75cc4e699f"))
        self.command_config = CommandConfigAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "Command_Config", "4727b5ad34b608")
        )

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> ShopClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> ShopClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
