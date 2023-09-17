from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from ._api.assets import AssetsAPI
from ._api.cdf_3_d_connection_properties import CdfConnectionPropertiesAPI
from ._api.cdf_3_d_entities import CdfEntitiesAPI
from ._api.cdf_3_d_models import CdfModelsAPI
from ._api.work_orders import WorksAPI
from ._api.work_items import WorksAPI


class ApmSimpleClient:
    """
    ApmSimpleClient

    Generated with:
        pygen = 0.18.0
        cognite-sdk = 6.25.1
        pydantic = 1.10.7

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
        self.assets = AssetsAPI(client)
        self.cdf_3_d_connection_properties = CdfConnectionPropertiesAPI(client)
        self.cdf_3_d_entities = CdfEntitiesAPI(client)
        self.cdf_3_d_models = CdfModelsAPI(client)
        self.work_orders = WorksAPI(client)
        self.work_items = WorksAPI(client)

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
