from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from markets.client._api.bids import BidsAPI
from markets.client._api.cog_bids import CogBidsAPI
from markets.client._api.cog_pools import CogPoolsAPI
from markets.client._api.cog_process import CogProcessAPI
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
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.bids = BidsAPI(client)
        self.cog_bids = CogBidsAPI(client)
        self.cog_pools = CogPoolsAPI(client)
        self.cog_process = CogProcessAPI(client)
        self.markets = MarketsAPI(client)
        self.process = ProcessAPI(client)
        self.value_transformations = ValueTransformationsAPI(client)


class PygenPoolAPIs:
    """
    PygenPoolAPIs

    Data Model:
        space: market
        externalId: PygenPool
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.bids = BidsAPI(client)
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
        pygen = 0.12.1
        cognite-sdk = 6.8.4
        pydantic = 2.0.3

    """

    def __init__(self, config: ClientConfig | None = None):
        client = CogniteClient(config)
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
    def from_toml(cls, file_path: Path | str) -> MarketClient:
        import toml

        return cls.azure_project(**toml.load(file_path)["cognite"])
