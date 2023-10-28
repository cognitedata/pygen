from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
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
        pygen = 0.27.1
        cognite-sdk = 6.37.0
        pydantic = 2.4.2

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
        self.asset = AssetAPI(client, dm.ViewId("tutorial_apm_simple", "Asset", "beb2bebdcbb4ad"))
        self.cdf_3_d_connection_properties = CdfConnectionPropertiesAPI(
            client, dm.ViewId("cdf_3d_schema", "Cdf3dConnectionProperties", "1")
        )
        self.cdf_3_d_entity = CdfEntityAPI(client, dm.ViewId("cdf_3d_schema", "Cdf3dEntity", "1"))
        self.cdf_3_d_model = CdfModelAPI(client, dm.ViewId("cdf_3d_schema", "Cdf3dModel", "1"))
        self.work_item = WorkItemAPI(client, dm.ViewId("tutorial_apm_simple", "WorkItem", "18ac48abbe96aa"))
        self.work_order = WorkOrderAPI(client, dm.ViewId("tutorial_apm_simple", "WorkOrder", "6f36e59c3c4896"))

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
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
