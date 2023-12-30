from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.blade import BladeAPI
from ._api.gearbox import GearboxAPI
from ._api.generator import GeneratorAPI
from ._api.high_speed_shaft import HighSpeedShaftAPI
from ._api.main_shaft import MainShaftAPI
from ._api.metmast import MetmastAPI
from ._api.nacelle import NacelleAPI
from ._api.power_inverter import PowerInverterAPI
from ._api.rotor import RotorAPI
from ._api.sensor_position import SensorPositionAPI
from ._api.windmill import WindmillAPI
from . import data_classes


class WindmillClient:
    """
    WindmillClient

    Generated with:
        pygen = 0.32.6
        cognite-sdk = 7.8.5
        pydantic = 1.10.7

    Data Model:
        space: power-models
        externalId: Windmill
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
        client.config.client_name = "CognitePygen:0.32.6"

        view_by_write_class = {
            data_classes.BladeApply: dm.ViewId("power-models", "Blade", "1"),
            data_classes.GearboxApply: dm.ViewId("power-models", "Gearbox", "1"),
            data_classes.GeneratorApply: dm.ViewId("power-models", "Generator", "1"),
            data_classes.HighSpeedShaftApply: dm.ViewId("power-models", "HighSpeedShaft", "1"),
            data_classes.MainShaftApply: dm.ViewId("power-models", "MainShaft", "1"),
            data_classes.MetmastApply: dm.ViewId("power-models", "Metmast", "1"),
            data_classes.NacelleApply: dm.ViewId("power-models", "Nacelle", "1"),
            data_classes.PowerInverterApply: dm.ViewId("power-models", "PowerInverter", "1"),
            data_classes.RotorApply: dm.ViewId("power-models", "Rotor", "1"),
            data_classes.SensorPositionApply: dm.ViewId("power-models", "SensorPosition", "1"),
            data_classes.WindmillApply: dm.ViewId("power-models", "Windmill", "1"),
        }

        self.blade = BladeAPI(client, view_by_write_class)
        self.gearbox = GearboxAPI(client, view_by_write_class)
        self.generator = GeneratorAPI(client, view_by_write_class)
        self.high_speed_shaft = HighSpeedShaftAPI(client, view_by_write_class)
        self.main_shaft = MainShaftAPI(client, view_by_write_class)
        self.metmast = MetmastAPI(client, view_by_write_class)
        self.nacelle = NacelleAPI(client, view_by_write_class)
        self.power_inverter = PowerInverterAPI(client, view_by_write_class)
        self.rotor = RotorAPI(client, view_by_write_class)
        self.sensor_position = SensorPositionAPI(client, view_by_write_class)
        self.windmill = WindmillAPI(client, view_by_write_class)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> WindmillClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> WindmillClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
