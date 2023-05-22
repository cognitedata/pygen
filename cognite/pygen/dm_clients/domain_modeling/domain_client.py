from __future__ import annotations

from threading import Lock
from typing import TYPE_CHECKING, Dict, Generic, Iterable, List, Optional, Type, TypeVar

from cachelib import BaseCache
from cognite.client import ClientConfig

from cognite.pygen.dm_clients.cdf.client_dm_v3 import CogniteClientDmV3, EdgesAPI, NodesAPI
from cognite.pygen.dm_clients.config import settings

from ..cdf.client_dm_v3 import ViewsAPI
from .domain_model import DomainModel

if TYPE_CHECKING:
    from .domain_model_api import DomainModelAPI
    from .schema import Schema


__all__ = [
    "DomainClient",
]

DomainModelT = TypeVar("DomainModelT", bound=DomainModel)


class DomainClient(Generic[DomainModelT]):
    """
    Base class for top-level domain client.

    Each schema type (registered with `@register_type` decorator) automatically gets a `DomainModelAPI` instance set on
    an attribute of this object.
    """

    def __init__(
        self,
        schema: Schema[DomainModelT],
        domain_model_api_class: Type[DomainModelAPI],
        cache: BaseCache,
        config: ClientConfig,
        space_id: Optional[str] = None,
        data_model: Optional[str] = None,
        schema_version: Optional[int] = None,
        # TODO ^ some of these args are redundant.
    ):
        # TODO make all these attributes "_private" to distinguish from domain model APIs
        self.schema = schema
        self._domain_model_api_class = domain_model_api_class
        self.cache: BaseCache = cache
        self._cache_lock: Lock = Lock()
        self._client = CogniteClientDmV3(config)
        self._client._config.headers["cdf-version"] = "alpha"
        if space_id is None:
            space_id = settings.dm_clients.space
        self.space_id = space_id
        self._data_model = data_model or settings.dm_clients.get("datamodel")
        self.schema_version = schema_version or settings.dm_clients.get("schema_version")
        if self.schema_version is None:
            raise NotImplementedError("Please specify the schema version")
            # TODO find latest version of the data model
        self._api_map: Dict[Type[DomainModelT], str] = {}

        # -------------------------
        # Todo why are these recreated? they already exists in self._client
        nodes_api = NodesAPI(
            self._client.config,
            self._client._API_VERSION,
            self._client,
        )
        edges_api = EdgesAPI(
            self._client.config,
            self._client._API_VERSION,
            self._client,
        )

        views_api = ViewsAPI(config, self._client._API_VERSION, self._client)
        views = {view.externalId: view for view in views_api.list(self.space_id)}
        # ------------------------

        api_args = {
            "nodes_api": nodes_api,
            "edges_api": edges_api,
            "domain_client": self,
            "space_id": self.space_id,
            "schema_version": self.schema_version,
            "api_version": self._client._API_VERSION,
        }

        for api_attr_name, domain_model in self.schema.types_map.items():
            # dynamically create a subclass of DomainModelAPI and assign it to an attribute of self:
            domain_model_name: str = domain_model.__name__
            api_class: Type[DomainModelAPI] = type(f"{domain_model_name}API", (self._domain_model_api_class,), {})
            self._api_map[domain_model] = api_attr_name
            setattr(self, api_attr_name, api_class(domain_model, views[domain_model_name], **api_args))  # type: ignore

    def get_api_for_item(self, item: DomainModelT) -> DomainModelAPI[DomainModelT]:
        return self.get_api_for_domain_model(type(item))

    def get_api_for_domain_model(self, domain_model: Type[DomainModelT]) -> DomainModelAPI[DomainModelT]:
        for registered_domain_model, self_attr_name in self._api_map.items():
            if registered_domain_model == domain_model:
                domain_model_api: DomainModelAPI[DomainModelT] = getattr(self, self_attr_name)
                return domain_model_api
        raise ValueError(f"No DomainModelAPI registered for {domain_model}")

    def apply(self, items: Iterable[DomainModelT], ext_id_prefix: str = "") -> List[DomainModelT]:
        """
        Given a list of nodes, figure out which DomainModelAPI to call and do it.
        Used by DomainModelAPI instances to create nested instances (without needing figure out the nested type).
        Mixed types in `nodes` not supported!
        """
        items = list(items)
        if not items:
            return []
        if len(types := {type(item) for item in items}) > 1:
            raise ValueError(
                f"Mixed domain models not supported in DomainClient.create! Got: {','.join(str(t) for t in types)}."
            )
        domain_model_api = self.get_api_for_item(items[0])
        return domain_model_api.apply(items, ext_id_prefix=ext_id_prefix)

    def delete(self, items: Iterable[DomainModelT], delete_related_items: bool = False):
        """
        Given a list of nodes, figure out which DomainModelAPI to call and do it.
        Used by DomainModelAPI instances to delete nested instances (without needing figure out the nested type).
        Mixed types in `nodes` not supported!
        """
        items = list(items)
        if not items:
            return
        if len(types := {type(item) for item in items}) > 1:
            raise ValueError(
                f"Mixed types not supported in DomainClient.delete! Got: {','.join(str(t) for t in types)}."
            )
        domain_model_api = self.get_api_for_item(items[0])
        domain_model_api.delete(items, delete_related_items)

    def graph(self, query: str):
        return self._client.graph(self.space_id, self._data_model, str(self.schema_version), query)


def get_empty_domain_client():
    from cachelib import SimpleCache

    from cognite.pygen.dm_clients.cdf.get_client import get_client_config
    from cognite.pygen.dm_clients.config import settings
    from cognite.pygen.dm_clients.domain_modeling.domain_model_api import DomainModelAPI
    from cognite.pygen.dm_clients.domain_modeling.schema import Schema

    return DomainClient(
        schema=Schema(),
        domain_model_api_class=DomainModelAPI,
        cache=SimpleCache(),
        config=get_client_config(),
        space_id=settings.dm_clients.space,
        data_model=settings.dm_clients.datamodel,
        schema_version=settings.dm_clients.schema_version,
    )
