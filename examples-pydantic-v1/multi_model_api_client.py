from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.blade import BladeAPI
from ._api.cdf_external_references import CDFExternalReferencesAPI
from ._api.cdf_external_references_listed import CDFExternalReferencesListedAPI
from ._api.connection_item_a import ConnectionItemAAPI
from ._api.connection_item_b import ConnectionItemBAPI
from ._api.connection_item_c import ConnectionItemCAPI
from ._api.empty import EmptyAPI
from ._api.gearbox import GearboxAPI
from ._api.generator import GeneratorAPI
from ._api.high_speed_shaft import HighSpeedShaftAPI
from ._api.implementation_1 import Implementation1API
from ._api.implementation_1_non_writeable import Implementation1NonWriteableAPI
from ._api.implementation_2 import Implementation2API
from ._api.main_interface import MainInterfaceAPI
from ._api.main_shaft import MainShaftAPI
from ._api.metmast import MetmastAPI
from ._api.nacelle import NacelleAPI
from ._api.power_inverter import PowerInverterAPI
from ._api.primitive_nullable import PrimitiveNullableAPI
from ._api.primitive_nullable_listed import PrimitiveNullableListedAPI
from ._api.primitive_required import PrimitiveRequiredAPI
from ._api.primitive_required_listed import PrimitiveRequiredListedAPI
from ._api.primitive_with_defaults import PrimitiveWithDefaultsAPI
from ._api.rotor import RotorAPI
from ._api.sensor_position import SensorPositionAPI
from ._api.sub_interface import SubInterfaceAPI
from ._api.windmill import WindmillAPI
from . import data_classes


class WindmillAPIs:
    """
    WindmillAPIs

    Data Model:
        space: power-models
        externalId: Windmill
        version: 1

    """

    def __init__(self, client: CogniteClient):
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


class OmniAPIs:
    """
    OmniAPIs

    Data Model:
        space: pygen-models
        externalId: Omni
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.CDFExternalReferences: dm.ViewId("pygen-models", "CDFExternalReferences", "1"),
            data_classes.CDFExternalReferencesListed: dm.ViewId("pygen-models", "CDFExternalReferencesListed", "1"),
            data_classes.ConnectionItemA: dm.ViewId("pygen-models", "ConnectionItemA", "1"),
            data_classes.ConnectionItemB: dm.ViewId("pygen-models", "ConnectionItemB", "1"),
            data_classes.ConnectionItemC: dm.ViewId("pygen-models", "ConnectionItemC", "1"),
            data_classes.Empty: dm.ViewId("pygen-models", "Empty", "1"),
            data_classes.Implementation1: dm.ViewId("pygen-models", "Implementation1", "1"),
            data_classes.Implementation1NonWriteable: dm.ViewId("pygen-models", "Implementation1NonWriteable", "1"),
            data_classes.Implementation2: dm.ViewId("pygen-models", "Implementation2", "1"),
            data_classes.MainInterface: dm.ViewId("pygen-models", "MainInterface", "1"),
            data_classes.PrimitiveNullable: dm.ViewId("pygen-models", "PrimitiveNullable", "1"),
            data_classes.PrimitiveNullableListed: dm.ViewId("pygen-models", "PrimitiveNullableListed", "1"),
            data_classes.PrimitiveRequired: dm.ViewId("pygen-models", "PrimitiveRequired", "1"),
            data_classes.PrimitiveRequiredListed: dm.ViewId("pygen-models", "PrimitiveRequiredListed", "1"),
            data_classes.PrimitiveWithDefaults: dm.ViewId("pygen-models", "PrimitiveWithDefaults", "1"),
            data_classes.SubInterface: dm.ViewId("pygen-models", "SubInterface", "1"),
        }

        self.cdf_external_references = CDFExternalReferencesAPI(client, view_by_read_class)
        self.cdf_external_references_listed = CDFExternalReferencesListedAPI(client, view_by_read_class)
        self.connection_item_a = ConnectionItemAAPI(client, view_by_read_class)
        self.connection_item_b = ConnectionItemBAPI(client, view_by_read_class)
        self.connection_item_c = ConnectionItemCAPI(client, view_by_read_class)
        self.empty = EmptyAPI(client, view_by_read_class)
        self.implementation_1 = Implementation1API(client, view_by_read_class)
        self.implementation_1_non_writeable = Implementation1NonWriteableAPI(client, view_by_read_class)
        self.implementation_2 = Implementation2API(client, view_by_read_class)
        self.main_interface = MainInterfaceAPI(client, view_by_read_class)
        self.primitive_nullable = PrimitiveNullableAPI(client, view_by_read_class)
        self.primitive_nullable_listed = PrimitiveNullableListedAPI(client, view_by_read_class)
        self.primitive_required = PrimitiveRequiredAPI(client, view_by_read_class)
        self.primitive_required_listed = PrimitiveRequiredListedAPI(client, view_by_read_class)
        self.primitive_with_defaults = PrimitiveWithDefaultsAPI(client, view_by_read_class)
        self.sub_interface = SubInterfaceAPI(client, view_by_read_class)


class MultiModelClient:
    """
    MultiModelClient

    Generated with:
        pygen = 0.32.6
        cognite-sdk = 7.8.5
        pydantic = 1.10.7

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

        self.windmill = WindmillAPIs(client)
        self.omni = OmniAPIs(client)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> MultiModelClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> MultiModelClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)