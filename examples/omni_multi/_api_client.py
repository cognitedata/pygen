from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.implementation_1_s_pygen_models import Implementation1sPygenModelsAPI
from ._api.implementation_1_s_pygen_models_other import Implementation1sPygenModelsOtherAPI
from ._api.implementation_1_v_2 import Implementation1v2API
from ._api.main_interface import MainInterfaceAPI
from ._api.sub_interface import SubInterfaceAPI
from . import data_classes


class OmniMultiAAPIs:
    """
    OmniMultiAAPIs

    Data Model:
        space: pygen-models
        externalId: OmniMultiA
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Implementation1sPygenModels: dm.ViewId("pygen-models", "Implementation1", "1"),
            data_classes.MainInterface: dm.ViewId("pygen-models", "MainInterface", "1"),
            data_classes.SubInterface: dm.ViewId("pygen-models", "SubInterface", "1"),
        }

        self.implementation_1_s_pygen_models = Implementation1sPygenModelsAPI(client, view_by_read_class)
        self.main_interface = MainInterfaceAPI(client, view_by_read_class)
        self.sub_interface = SubInterfaceAPI(client, view_by_read_class)


class OmniMultiBAPIs:
    """
    OmniMultiBAPIs

    Data Model:
        space: pygen-models
        externalId: OmniMultiB
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Implementation1v2: dm.ViewId("pygen-models", "Implementation1", "2"),
            data_classes.MainInterface: dm.ViewId("pygen-models", "MainInterface", "1"),
            data_classes.SubInterface: dm.ViewId("pygen-models", "SubInterface", "1"),
        }

        self.implementation_1_v_2 = Implementation1v2API(client, view_by_read_class)
        self.main_interface = MainInterfaceAPI(client, view_by_read_class)
        self.sub_interface = SubInterfaceAPI(client, view_by_read_class)


class OmniMultiCAPIs:
    """
    OmniMultiCAPIs

    Data Model:
        space: pygen-models
        externalId: OmniMultiC
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Implementation1sPygenModelsOther: dm.ViewId("pygen-models-other", "Implementation1", "1"),
        }

        self.implementation_1_s_pygen_models_other = Implementation1sPygenModelsOtherAPI(client, view_by_read_class)


class OmniMultiClient:
    """
    OmniMultiClient

    Generated with:
        pygen = 0.35.4
        cognite-sdk = 7.13.6
        pydantic = 2.5.3

    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        # The client name is used for aggregated logging of Pygen Usage
        client.config.client_name = "CognitePygen:0.35.4"

        self.omni_multi_a = OmniMultiAAPIs(client)
        self.omni_multi_b = OmniMultiBAPIs(client)
        self.omni_multi_c = OmniMultiCAPIs(client)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> OmniMultiClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> OmniMultiClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
