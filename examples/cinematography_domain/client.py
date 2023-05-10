"""
This file is auto-generated using `dm topython` CLI tool.
"""

from __future__ import annotations

from typing import Optional

from cachelib import BaseCache, SimpleCache
from cognite.client import ClientConfig

from cognite.dm_clients.cdf.get_client import get_client_config
from cognite.dm_clients.config import settings
from cognite.dm_clients.domain_modeling import DomainClient, DomainModelAPI

from .schema import Movie, Person, cine_schema


class CineClient(DomainClient):
    """Domain-specific client class for the 'Cine'."""

    movie: DomainModelAPI[Movie]
    person: DomainModelAPI[Person]


def get_cine_client(
    cache: Optional[BaseCache] = None,
    space_id: Optional[str] = None,
    data_model: Optional[str] = None,
    schema_version: Optional[int] = None,
    config: Optional[ClientConfig] = None,
) -> CineClient:
    """Quick way of instantiating a CineClient with sensible defaults for development."""
    cache = cache if cache is not None else SimpleCache()
    config = config if config is not None else get_client_config()
    space_external_id = space_id or settings.dm_clients.space
    data_model = data_model or settings.dm_clients.datamodel
    schema_version = schema_version or settings.dm_clients.schema_version
    return CineClient(
        cine_schema,
        DomainModelAPI,
        cache,
        config,
        space_external_id,
        data_model,
        schema_version,
    )
