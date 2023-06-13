from __future__ import annotations

from typing import Optional

from cachelib import BaseCache, SimpleCache
from pygen.dm_clients.config import settings

from cognite.pygen.dm_clients.cdf.get_client import CogniteConfig, get_client_config
from cognite.pygen.dm_clients.domain_modeling import DomainClient, DomainModelAPI

from .schema import Movie, Person, cine_schema


class CineClient(DomainClient):
    """
    Domain-specific client class for the entire domain.
    This class can contain any domain-specific logic, but is entirely optional. The attributes below (`actor` and
    `movie`) are created in the base class, and here we have only added type annotations so that IntelliSense can work.
    """

    movie: DomainModelAPI[Movie]
    person: DomainModelAPI[Person]


def get_cine_client(
    cache: Optional[BaseCache] = None,
    space: Optional[str] = None,
    data_model: Optional[str] = None,
    schema_version: Optional[int] = None,
    cognite_config: Optional[CogniteConfig] = None,
) -> CineClient:
    """Quick way of instantiating a CineClient, taking default values from settings."""
    return CineClient(
        cine_schema,
        DomainModelAPI,
        cache or SimpleCache(),
        get_client_config(cognite_config),
        space or settings.get("dm_clients.space"),
        data_model or settings.get("dm_clients.datamodel"),
        schema_version or settings.get("dm_clients.schema_version"),
    )
