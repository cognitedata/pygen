from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.scenario_instance import ScenarioInstanceAPI
from . import data_classes


class ScenarioInstanceClient:
    """
    ScenarioInstanceClient

    Generated with:
        pygen = 0.31.0
        cognite-sdk = 7.5.1
        pydantic = 1.10.7

    Data Model:
        space: IntegrationTestsImmutable
        externalId: ScenarioInstance
        version: 1
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        view_by_write_class = {
            data_classes.ScenarioInstanceApply: dm.ViewId(
                "IntegrationTestsImmutable", "ScenarioInstance", "ee2b79fd98b5bb"
            ),
        }

        self.scenario_instance = ScenarioInstanceAPI(client, view_by_write_class)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> ScenarioInstanceClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> ScenarioInstanceClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
