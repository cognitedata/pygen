from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from shop_pydantic_v1.client._api.cases import CasesAPI
from shop_pydantic_v1.client._api.command_configs import CommandConfigsAPI


class ShopClient:
    """
    ShopClient

    Generated with:
        pygen = 0.17.4
        cognite-sdk = 6.14.0
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
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> ShopClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError:
                raise ValueError(f"Could not find section '{section}' in {file_path}")

        return cls.azure_project(**toml_content)
