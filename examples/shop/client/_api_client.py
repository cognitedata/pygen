from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from shop.client._api.cases import CasesAPI
from shop.client._api.command_configs import CommandConfigsAPI


class ShopClient:
    """
    ShopClient

    Generated with:
        pygen = 0.12.0
        cognite-sdk = 6.8.4
        pydantic = 2.0.3

    Data Model:
        space: IntegrationTestsImmutable
        externalId: SHOP_Model
        version: 2
    """

    def __init__(self, config: ClientConfig | None = None):
        client = CogniteClient(config)
        self.cases = CasesAPI(client)
        self.command_configs = CommandConfigsAPI(client)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> ShopClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str) -> ShopClient:
        import toml

        return cls.azure_project(**toml.load(file_path)["cognite"])
