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
        pygen = 0.35.3
        cognite-sdk = 7.13.6
        pydantic = 2.5.3

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
        client.config.client_name = "CognitePygen:0.35.3"

        view_by_read_class = {
            data_classes.Blade: dm.ViewId("power-models", "Blade", "1"),
            data_classes.Gearbox: dm.ViewId("power-models", "Gearbox", "1"),
            data_classes.Generator: dm.ViewId("power-models", "Generator", "1"),
            data_classes.HighSpeedShaft: dm.ViewId("power-models", "HighSpeedShaft", "1"),
            data_classes.MainShaft: dm.ViewId("power-models", "MainShaft", "1"),
            data_classes.Metmast: dm.ViewId("power-models", "Metmast", "1"),
            data_classes.Nacelle: dm.ViewId("power-models", "Nacelle", "1"),
            data_classes.PowerInverter: dm.ViewId("power-models", "PowerInverter", "1"),
            data_classes.Rotor: dm.ViewId("power-models", "Rotor", "1"),
            data_classes.SensorPosition: dm.ViewId("power-models", "SensorPosition", "1"),
            data_classes.Windmill: dm.ViewId("power-models", "Windmill", "1"),
        }

        self.blade = BladeAPI(client, view_by_read_class)
        self.gearbox = GearboxAPI(client, view_by_read_class)
        self.generator = GeneratorAPI(client, view_by_read_class)
        self.high_speed_shaft = HighSpeedShaftAPI(client, view_by_read_class)
        self.main_shaft = MainShaftAPI(client, view_by_read_class)
        self.metmast = MetmastAPI(client, view_by_read_class)
        self.nacelle = NacelleAPI(client, view_by_read_class)
        self.power_inverter = PowerInverterAPI(client, view_by_read_class)
        self.rotor = RotorAPI(client, view_by_read_class)
        self.sensor_position = SensorPositionAPI(client, view_by_read_class)
        self.windmill = WindmillAPI(client, view_by_read_class)

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

    def _repr_html_(self) -> str:
        return """<strong>WindmillClient</strong> generated from data model ("power-models", "Windmill", "1")<br />
with the following APIs available<br />
&nbsp;&nbsp;&nbsp;&nbsp;.blade<br />
&nbsp;&nbsp;&nbsp;&nbsp;.gearbox<br />
&nbsp;&nbsp;&nbsp;&nbsp;.generator<br />
&nbsp;&nbsp;&nbsp;&nbsp;.high_speed_shaft<br />
&nbsp;&nbsp;&nbsp;&nbsp;.main_shaft<br />
&nbsp;&nbsp;&nbsp;&nbsp;.metmast<br />
&nbsp;&nbsp;&nbsp;&nbsp;.nacelle<br />
&nbsp;&nbsp;&nbsp;&nbsp;.power_inverter<br />
&nbsp;&nbsp;&nbsp;&nbsp;.rotor<br />
&nbsp;&nbsp;&nbsp;&nbsp;.sensor_position<br />
&nbsp;&nbsp;&nbsp;&nbsp;.windmill<br />
"""
