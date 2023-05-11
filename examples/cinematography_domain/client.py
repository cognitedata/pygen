from __future__ import annotations

from typing import Optional

from cachelib import BaseCache, SimpleCache

from cognite.dm_clients.cdf.get_client import get_client_config
from cognite.dm_clients.domain_modeling import DomainClient, DomainModelAPI

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
    space_id: Optional[str] = None,
    data_model: Optional[str] = None,
    schema_version: Optional[int] = None,
) -> CineClient:
    """Quick way of instantiating a CineClient with sensible defaults for development."""
    cache = SimpleCache() if cache is None else cache
    config = get_client_config()
    return CineClient(cine_schema, DomainModelAPI, cache, config, space_id, data_model, schema_version)
