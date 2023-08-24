from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from markets.client._api.bids import BidsAPI
from markets.client._api.cog_bids import CogBidsAPI
from markets.client._api.cog_pools import CogPoolsAPI
from markets.client._api.cog_process import CogProcessAPI
from markets.client._api.date_transformations import DateTransformationsAPI
from markets.client._api.date_transformation_pairs import DateTransformationPairsAPI
from markets.client._api.markets import MarketsAPI
from markets.client._api.process import ProcessAPI
from markets.client._api.pygen_bids import PygenBidsAPI
from markets.client._api.pygen_pools import PygenPoolsAPI
from markets.client._api.pygen_process import PygenProcessAPI
from markets.client._api.value_transformations import ValueTransformationsAPI


class CogPoolAPIs:
    """
    CogPoolAPIs

    Data Model:
        space: market
        externalId: CogPool
        version: 3

    """

    def __init__(self, client: CogniteClient):
        self.bids = BidsAPI(client)
        self.cog_bids = CogBidsAPI(client)
        self.cog_pools = CogPoolsAPI(client)
        self.cog_process = CogProcessAPI(client)
        self.date_transformations = DateTransformationsAPI(client)
        self.date_transformation_pairs = DateTransformationPairsAPI(client)
        self.markets = MarketsAPI(client)
        self.process = ProcessAPI(client)
        self.value_transformations = ValueTransformationsAPI(client)


class PygenPoolAPIs:
    """
    PygenPoolAPIs

    Data Model:
        space: market
        externalId: PygenPool
        version: 3

    """

    def __init__(self, client: CogniteClient):
        self.bids = BidsAPI(client)
        self.date_transformations = DateTransformationsAPI(client)
        self.date_transformation_pairs = DateTransformationPairsAPI(client)
        self.markets = MarketsAPI(client)
        self.process = ProcessAPI(client)
        self.pygen_bids = PygenBidsAPI(client)
        self.pygen_pools = PygenPoolsAPI(client)
        self.pygen_process = PygenProcessAPI(client)
        self.value_transformations = ValueTransformationsAPI(client)


class MarketClient:
    """
    MarketClient

    Generated with:
        pygen = 0.17.4
        cognite-sdk = 6.14.0
        pydantic = 2.2.1

    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        self.cog_pool = CogPoolAPIs(client)
        self.pygen_pool = PygenPoolAPIs(client)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> MarketClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> MarketClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError:
                raise ValueError(f"Could not find section '{section}' in {file_path}")

        return cls.azure_project(**toml_content)
