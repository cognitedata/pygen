from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from ._api.bid import BidAPI
from ._api.cog_bid import CogBidAPI
from ._api.cog_pool import CogPoolAPI
from ._api.cog_process import CogProcessAPI
from ._api.date_transformation import DateTransformationAPI
from ._api.date_transformation_pair import DateTransformationPairAPI
from ._api.market import MarketAPI
from ._api.process import ProcessAPI
from ._api.pygen_bid import PygenBidAPI
from ._api.pygen_pool import PygenPoolAPI
from ._api.pygen_process import PygenProcessAPI
from ._api.value_transformation import ValueTransformationAPI


class CogPoolAPIs:
    """
    CogPoolAPIs

    Data Model:
        space: market
        externalId: CogPool
        version: 3

    """

    def __init__(self, client: CogniteClient):
        self.bid = BidAPI(client)
        self.cog_bid = CogBidAPI(client)
        self.cog_pool = CogPoolAPI(client)
        self.cog_process = CogProcessAPI(client)
        self.date_transformation = DateTransformationAPI(client)
        self.date_transformation_pair = DateTransformationPairAPI(client)
        self.market = MarketAPI(client)
        self.process = ProcessAPI(client)
        self.value_transformation = ValueTransformationAPI(client)


class PygenPoolAPIs:
    """
    PygenPoolAPIs

    Data Model:
        space: market
        externalId: PygenPool
        version: 3

    """

    def __init__(self, client: CogniteClient):
        self.bid = BidAPI(client)
        self.date_transformation = DateTransformationAPI(client)
        self.date_transformation_pair = DateTransformationPairAPI(client)
        self.market = MarketAPI(client)
        self.process = ProcessAPI(client)
        self.pygen_bid = PygenBidAPI(client)
        self.pygen_pool = PygenPoolAPI(client)
        self.pygen_process = PygenProcessAPI(client)
        self.value_transformation = ValueTransformationAPI(client)


class MarketClient:
    """
    MarketClient

    Generated with:
        pygen = 0.18.1
        cognite-sdk = 6.25.1
        pydantic = 2.3.0

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
