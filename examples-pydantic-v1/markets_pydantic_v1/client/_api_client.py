from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
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
        self.bid = BidAPI(client, dm.ViewId("market", "Bid", "1add47c48cf88b"))
        self.cog_bid = CogBidAPI(client, dm.ViewId("market", "CogBid", "3c04fa081c45d5"))
        self.cog_pool = CogPoolAPI(client, dm.ViewId("market", "CogPool", "28af312f1d7093"))
        self.cog_process = CogProcessAPI(client, dm.ViewId("market", "CogProcess", "b5df5d19e08fd0"))
        self.date_transformation = DateTransformationAPI(
            client, dm.ViewId("market", "DateTransformation", "482866112eb911")
        )
        self.date_transformation_pair = DateTransformationPairAPI(
            client, dm.ViewId("market", "DateTransformationPair", "bde9fd4428c26e")
        )
        self.market = MarketAPI(client, dm.ViewId("market", "Market", "a5067899750188"))
        self.process = ProcessAPI(client, dm.ViewId("market", "Process", "98a2becd0f63ee"))
        self.value_transformation = ValueTransformationAPI(
            client, dm.ViewId("market", "ValueTransformation", "147ebcf1583165")
        )


class PygenPoolAPIs:
    """
    PygenPoolAPIs

    Data Model:
        space: market
        externalId: PygenPool
        version: 3

    """

    def __init__(self, client: CogniteClient):
        self.bid = BidAPI(client, dm.ViewId("market", "Bid", "1ad3f030a8399f"))
        self.date_transformation = DateTransformationAPI(
            client, dm.ViewId("market", "DateTransformation", "5248f7e87c4c96")
        )
        self.date_transformation_pair = DateTransformationPairAPI(
            client, dm.ViewId("market", "DateTransformationPair", "310f933a9aca9b")
        )
        self.market = MarketAPI(client, dm.ViewId("market", "Market", "5b43e98565d4d5"))
        self.process = ProcessAPI(client, dm.ViewId("market", "Process", "b3e0207c0bb510"))
        self.pygen_bid = PygenBidAPI(client, dm.ViewId("market", "PygenBid", "57f9da2a1acf7e"))
        self.pygen_pool = PygenPoolAPI(client, dm.ViewId("market", "PygenPool", "23c71ba66bad9d"))
        self.pygen_process = PygenProcessAPI(client, dm.ViewId("market", "PygenProcess", "477b68a858c7a8"))
        self.value_transformation = ValueTransformationAPI(
            client, dm.ViewId("market", "ValueTransformation", "946587c592b44c")
        )


class MarketClient:
    """
    MarketClient

    Generated with:
        pygen = 0.20.4
        cognite-sdk = 6.25.3
        pydantic = 1.10.7

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
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
