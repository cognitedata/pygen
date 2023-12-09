from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.equipment_module import EquipmentModuleAPI
from ._api.unit_procedure import UnitProcedureAPI
from ._api.work_order import WorkOrderAPI
from . import data_classes


class EquipmentUnitClient:
    """
    EquipmentUnitClient

    Generated with:
        pygen = 0.32.0
        cognite-sdk = 7.5.1
        pydantic = 2.5.2

    Data Model:
        space: IntegrationTestsImmutable
        externalId: EquipmentUnit
        version: 2
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        view_by_write_class = {
            data_classes.EquipmentModuleApply: dm.ViewId(
                "IntegrationTestsImmutable", "EquipmentModule", "b1cd4bf14a7a33"
            ),
            data_classes.StartEndTimeApply: dm.ViewId("IntegrationTestsImmutable", "StartEndTime", "d416e0ed98186b"),
            data_classes.UnitProcedureApply: dm.ViewId("IntegrationTestsImmutable", "UnitProcedure", "a6e2fea1e1c664"),
            data_classes.WorkOrderApply: dm.ViewId("IntegrationTestsImmutable", "WorkOrder", "c5543fb2b1bc81"),
        }

        self.equipment_module = EquipmentModuleAPI(client, view_by_write_class)
        self.unit_procedure = UnitProcedureAPI(client, view_by_write_class)
        self.work_order = WorkOrderAPI(client, view_by_write_class)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> EquipmentUnitClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> EquipmentUnitClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
