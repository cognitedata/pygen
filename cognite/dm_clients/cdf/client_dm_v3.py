from __future__ import annotations

import logging
from contextlib import suppress
from pprint import pformat
from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence
from urllib.parse import urlencode

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api_client import APIClient
from cognite.client.exceptions import CogniteAPIError
from pydantic import parse_obj_as
from requests import Response
from retry import retry

from cognite.dm_clients.cdf.data_classes_dm_v3 import Container, DataModel, Edge, Node, Space, View
from cognite.dm_clients.cdf.get_client import get_client_config
from cognite.dm_clients.config import settings

logger = logging.getLogger(__name__)


HttpVerbT = Literal["GET", "PUT", "DELETE", "POST"]

_MAX_TRIES = int(settings.get("dm_clients.max_tries", 15))


class DataModelStorageAPI(APIClient):
    """Base for other API classes"""

    @property
    def url(self) -> str:
        return f"/api/v1/projects/{self._config.project}"

    def _post_to_endpoint(self, payload: dict, endpoint: str) -> dict:
        return self._retrieve_from_endpoint("POST", f"{self.url}{endpoint}", payload)

    def _get_from_endpoint(self, url_query: dict, endpoint: str) -> dict:
        return self._retrieve_from_endpoint("GET", f"{self.url}{endpoint}", url_query)

    @retry(CogniteAPIError, delay=1, backoff=2, max_delay=10, tries=_MAX_TRIES, logger=logger)
    def _retrieve_from_endpoint(self, method: Literal["GET", "POST"], url: str, data: dict) -> dict:
        """
        Request data from API, potentially making multiple request if `limit` is not set in `data`.
        For POST requests: `data` is sent as JSON in the body
        For GET requests: `data` is sent se url query params (url-encoded)
        """
        follow_cursor = data.get("limit") is None
        data = data.copy()
        response = self._make_request(method, url, data)
        response.raise_for_status()
        result = response.json()
        while follow_cursor and (cursor := result.get("nextCursor")):
            data["cursor"] = cursor
            another_response = self._make_request(method, url, data)
            another_response.raise_for_status()
            another_result = another_response.json()
            result["items"].extend(another_result["items"])
            result["nextCursor"] = another_result.get("nextCursor")
        with suppress(KeyError):
            del result["nextCursor"]
            del result["cursor"]
        logger.debug(f"{method} to {url}\ndata:\n{pformat(data)}\nresult:\n{pformat(result)}")
        return result

    def _make_request(
        self, method: Literal["GET", "POST"], url: str, data: Optional[Dict[str, Any]] = None
    ) -> Response:
        if data is not None:
            data = data.copy()
        if method == "POST":
            response = self._cognite_client.post(url, json=data or {})
        elif method == "GET":
            response = self._cognite_client.get(f"{url}?{urlencode(data or {})}")
        else:
            raise ValueError(f"Unsupported API method: {method}")
        return response


# SpacesAPI not currently used, but included here for completeness.
class SpacesAPI(DataModelStorageAPI):
    @property
    def url(self) -> str:
        return f"{super().url}/models/spaces"

    @staticmethod
    def _payload_space_ids(space_ids: Iterable[str]) -> dict:
        """API payload for `list` and `delete` endpoints."""
        return {"items": [{"space": space_id} for space_id in space_ids]}

    @staticmethod
    def _payload_items(spaces: Iterable[Space]) -> dict:
        """API payload for `apply` endpoint."""
        return {"items": [space.dict() for space in spaces]}

    @staticmethod
    def _parse(result: dict) -> List[Space]:
        return parse_obj_as(List[Space], result["items"])

    def list(self, limit: int = 1000) -> List[Space]:
        return self._parse(self._get_from_endpoint({"limit": limit}, ""))

    def retrieve(self, external_ids: Iterable[str]) -> List[Space]:
        _ext_ids = list(external_ids)
        if not _ext_ids:
            return []
        return self._parse(self._post_to_endpoint(self._payload_space_ids(_ext_ids), "/byids"))

    def apply(self, spaces: Iterable[Space]) -> List[Space]:
        _spaces = list(spaces)
        if not _spaces:
            return []
        return self._parse(self._post_to_endpoint(self._payload_items(_spaces), ""))

    def delete(self, external_ids: Iterable[str]) -> None:
        _ext_ids = list(external_ids)
        if not _ext_ids:
            return
        self._post_to_endpoint(self._payload_space_ids(_ext_ids), "/delete")


# DataModelAPI not currently used, but included here for completeness.
class DataModelAPI(DataModelStorageAPI):
    @property
    def url(self) -> str:
        return f"{super().url}/models/datamodels"

    @staticmethod
    def _payload_ext_ids(space_id: str, external_ids: Iterable[str]) -> dict:
        """API payload for `list` and `delete` endpoints."""
        return {"items": [{"space": space_id, "externalId": ext_id} for ext_id in external_ids]}

    @staticmethod
    def _payload_items(data_models: Iterable[DataModel]) -> dict:
        """API payload for `apply` endpoint."""
        return {"items": [data_model.dict() for data_model in data_models]}

    @staticmethod
    def _payload_delete(data_models: Iterable[DataModel]) -> dict:
        """API payload for `delete` endpoint."""
        return {
            "items": [
                {"space": data_model.space, "version": data_model.version, "externalId": data_model.externalId}
                for data_model in data_models
            ],
        }

    @staticmethod
    def _parse(result: dict) -> List[DataModel]:
        return parse_obj_as(List[DataModel], result["items"])

    def list(self, space_id: str, limit: int = 1000) -> List[DataModel]:
        return self._parse(self._get_from_endpoint({"space": space_id, "limit": limit}, ""))

    def retrieve(self, space_id: str, external_ids: Iterable[str]) -> List[DataModel]:
        _ext_ids = list(external_ids)
        if not _ext_ids:
            return []
        return self._parse(self._post_to_endpoint(self._payload_ext_ids(space_id, _ext_ids), "/byids"))

    def apply(self, data_models: Iterable[DataModel]) -> List[DataModel]:
        _data_models = list(data_models)
        if not _data_models:
            return []
        return self._parse(self._post_to_endpoint(self._payload_items(_data_models), ""))

    def delete(self, data_models: Iterable[DataModel]) -> None:
        _data_models = list(data_models)
        if not _data_models:
            return
        self._post_to_endpoint(self._payload_delete(_data_models), "/delete")


# ViewsAPI not currently used, but included here for completeness.
class ViewsAPI(DataModelStorageAPI):
    @property
    def url(self) -> str:
        return f"{super().url}/models/views"

    @staticmethod
    def _payload_ext_ids(space_id: str, external_ids: Iterable[str], version: Optional[int] = None) -> dict:
        """API payload for `list` and `delete` endpoints."""
        items = [{"space": space_id, "externalId": ext_id} for ext_id in external_ids]
        if version is not None:
            for item in items:
                item["version"] = f"{version}"
        return {"items": items}

    @staticmethod
    def _payload_items(views: Iterable[View]) -> dict:
        """API payload for `apply` endpoint."""
        return {"items": [view.dict() for view in views]}

    @staticmethod
    def _payload_delete(views: Iterable[View]) -> dict:
        """API payload for `delete` endpoint."""
        return {
            "items": [{"space": view.space, "version": view.version, "externalId": view.externalId} for view in views],
        }

    @staticmethod
    def _parse(result: dict) -> List[View]:
        return parse_obj_as(List[View], result["items"])

    def list(self, space_id: str, version: Optional[int] = None, limit: int = 1000) -> List[View]:
        views = self._parse(self._get_from_endpoint({"space": space_id, "limit": limit, "allVersions": True}, ""))
        return [view for view in views if version is None or view.version == f"{version}"]

    def retrieve(self, space_id: str, external_ids: Iterable[str], version: Optional[int] = None) -> List[View]:
        _ext_ids = list(external_ids)
        if not _ext_ids:
            return []
        return self._parse(self._post_to_endpoint(self._payload_ext_ids(space_id, _ext_ids, version), "/byids"))

    def apply(self, views: Iterable[View]) -> List[View]:
        _views = list(views)
        if not _views:
            return []
        return self._parse(self._post_to_endpoint(self._payload_items(_views), ""))

    def delete_views(self, views: Iterable[View]) -> None:
        _views = list(views)
        if not _views:
            return
        self._post_to_endpoint(self._payload_delete(_views), "/delete")

    def delete(self, space_id: str, version: str, external_ids: Iterable[str]) -> None:
        _ext_ids = list(external_ids)
        if not _ext_ids:
            return
        views = parse_obj_as(
            List[View],
            [{"space": space_id, "version": version, "externalId": ext_id} for ext_id in _ext_ids],
        )
        self.delete_views(views)


# ContainersAPI not currently used, but included here for completeness.
class ContainersAPI(DataModelStorageAPI):
    @property
    def url(self) -> str:
        return f"{super().url}/models/containers"

    @staticmethod
    def _payload_ext_ids(space_id: str, external_ids: Iterable[str]) -> dict:
        """API payload for `list` and `delete` endpoints."""
        return {"items": [{"space": space_id, "externalId": ext_id} for ext_id in external_ids]}

    @staticmethod
    def _payload_items(containers: Iterable[Container]) -> dict:
        """API payload for `apply` endpoint."""
        return {"items": [container.dict() for container in containers]}

    @staticmethod
    def _parse(result: dict) -> List[Container]:
        return parse_obj_as(List[Container], result["items"])

    def list(self, space_id: str, limit: int = 1000) -> List[Container]:
        return self._parse(self._get_from_endpoint({"space": space_id, "limit": limit}, ""))

    def retrieve(self, space_id: str, external_ids: Iterable[str]) -> List[Container]:
        _ext_ids = list(external_ids)
        if not _ext_ids:
            return []
        return self._parse(self._post_to_endpoint(self._payload_ext_ids(space_id, _ext_ids), "/byids"))

    def apply(self, containers: Iterable[Container]) -> List[Container]:
        _containers = list(containers)
        if not _containers:
            return []
        return self._parse(self._post_to_endpoint(self._payload_items(_containers), ""))

    def delete(self, space_id: str, external_ids: Iterable[str]) -> None:
        _ext_ids = list(external_ids)
        if not _ext_ids:
            return
        self._post_to_endpoint(self._payload_ext_ids(space_id, _ext_ids), "/delete")


class NodesAPI(DataModelStorageAPI):
    @property
    def url(self) -> str:
        return f"{super().url}/models/instances"

    @staticmethod
    def _parse(result: dict) -> List[Node]:
        return parse_obj_as(List[Node], result["items"])

    @staticmethod
    def _payload_item(space: str, external_id: str) -> dict:
        """
        Part of payloads that references a node by its externalId.
        Used in retrieve, apply, and delete.
        """
        return {
            "instanceType": "node",
            "space": space,
            "externalId": external_id,
        }

    @staticmethod
    def _payload_view_source(view: View) -> dict:
        """
        Part of payloads that references a source view.
        Used in list, retrieve, and apply.
        """
        return {
            "source": {
                "type": "view",
                "space": view.space,
                "externalId": view.externalId,
                "version": view.version,
            },
        }

    def list(self, view: View, limit: int = 1000) -> List[Node]:
        payload = {
            "limit": limit,
            "instanceType": "node",
            "sources": [self._payload_view_source(view)],
        }
        return self._parse(self._post_to_endpoint(payload, "/list"))

    def retrieve(self, view: View, external_ids: Iterable[str]) -> List[Node]:
        _ext_ids = list(external_ids)
        if not _ext_ids:
            return []
        payload = {
            "sources": [self._payload_view_source(view)],
            "items": [self._payload_item(view.space, ext_id) for ext_id in _ext_ids],
        }
        return self._parse(self._post_to_endpoint(payload, "/byids"))

    def apply(self, view: View, nodes: Iterable[Node]) -> None:
        _nodes = list(nodes)
        if not _nodes:
            return
        payload = {
            "replace": True,
            "items": [
                {
                    **self._payload_item(node.space, node.externalId),
                    "sources": [
                        {
                            **self._payload_view_source(view),
                            "properties": (node.properties or {}).get(view.space, {}).get(view.externalId, {}),
                        },
                    ],
                }
                for node in _nodes
            ],
        }
        self._parse(self._post_to_endpoint(payload, ""))
        # Note: API response contains partial nodes: They don't contain properties.

    def delete(self, space: str, external_ids: Iterable[str]) -> None:
        _ext_ids = list(external_ids)
        if not _ext_ids:
            return
        payload = {"items": [self._payload_item(space, ext_id) for ext_id in _ext_ids]}
        self._post_to_endpoint(payload, "/delete")


class EdgesAPI(DataModelStorageAPI):
    @property
    def url(self) -> str:
        return f"{super().url}/models/instances"

    @staticmethod
    def _parse(result: dict) -> List[Edge]:
        return parse_obj_as(List[Edge], result["items"])

    @staticmethod
    def _payload_item(space: str, external_id: str) -> dict:
        """
        Part of payloads that references an edge by its externalId.
        Used in retrieve, apply, and delete.
        """
        return {
            "instanceType": "edge",
            "space": space,
            "externalId": external_id,
        }

    def list(
        self,
        node_view: View,
        attributes: Optional[Sequence[str]] = None,
        start_external_ids: Optional[Sequence[str]] = None,
        limit: int = 1000,
    ) -> List[Edge]:
        """
        List edges for "outwards" relationships from the `node_view`, i.e. their startNode is an instance from the view.
        Optionally, further restrict the query with:
         * attributes: list only edges that describe relation on the given set of attributes,
         * start_external_ids: list only edges that have `startNode` matching one of the given set of externalIds
        """
        if attributes is None:
            # query edges for all attributes
            attributes = [attr for attr, prop in node_view.properties.items() if prop.get("direction") == "outwards"]
            # TODO ^ make sure the API doesn't choke on empty "values" list in filter below!

        if not attributes:
            return []

        filter_ = {
            "and": [
                {
                    "in": {
                        "property": ["edge", "type"],
                        "values": [
                            [node_view.space, node_view.properties[attr]["type"]["externalId"]] for attr in attributes
                        ],
                    },
                },
            ],
        }
        if start_external_ids is not None and not start_external_ids:
            # empty list, that's different from None!
            # Passing in None means "gimme for all", but passing in [] means "gimme for these 0 elements",
            # which we shall do:
            return []

        if start_external_ids:
            filter_["and"].append(
                {
                    "in": {
                        "property": ["edge", "startNode"],
                        "values": [[node_view.space, ext_id] for ext_id in start_external_ids],
                    },
                }
            )

        payload = {
            "instanceType": "edge",
            "limit": limit,
            "filter": filter_,
        }
        return self._parse(self._post_to_endpoint(payload, "/list"))

    def retrieve(self, space: str, external_ids: Iterable[str]) -> List[Edge]:
        _ext_ids = list(external_ids)
        if not _ext_ids:
            return []
        payload = {"items": [self._payload_item(space, ext_id) for ext_id in _ext_ids]}
        return self._parse(self._post_to_endpoint(payload, "/byids"))

    def apply(self, edges: Iterable[Edge]) -> None:
        _edges = list(edges)
        if not _edges:
            return
        payload = {
            "replace": True,
            "items": [
                {
                    **self._payload_item(edge.space, edge.externalId),
                    "startNode": edge.startNode.dict(),
                    "endNode": edge.endNode.dict(),
                    "type": edge.type.dict(),
                }
                for edge in _edges
            ],
        }
        self._post_to_endpoint(payload, "")

    def delete(self, space: str, external_ids: Iterable[str]) -> None:
        _ext_ids = list(external_ids)
        if not _ext_ids:
            return
        payload = {"items": [self._payload_item(space, ext_id) for ext_id in _ext_ids]}
        self._post_to_endpoint(payload, "/delete")


class CogniteClientDmV3(CogniteClient):
    def __init__(self, config: ClientConfig):
        # config.headers["cdf-version"] = "alpha"
        super().__init__(config)
        self.spaces = SpacesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.datamodels = DataModelAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.views = ViewsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.containers = ContainersAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.nodes = NodesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.edges = EdgesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)

    def graph(self, space: str, datamodel: str, version: str, query: str):
        return self.post(
            f"/api/v1/projects/{self.config.project}/userapis"
            f"/spaces/{space}/datamodels/{datamodel}/versions/{version}/graphql",
            json={"query": query},
        ).json()


def get_cognite_client_dm_v3() -> CogniteClientDmV3:
    client_config = get_client_config()
    return CogniteClientDmV3(client_config)
