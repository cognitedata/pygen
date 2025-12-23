import concurrent.futures
from collections.abc import Sequence
from typing import Any, Literal

from pydantic import JsonValue, TypeAdapter

from cognite.pygen._client import PygenClientConfig
from cognite.pygen._generation.python.instance_api.http_client import HTTPClient, RequestMessage
from cognite.pygen._utils.collection import chunker_sequence

from ._instance import InstanceId, InstanceModel, InstanceResult, InstanceWrite


class InstanceClient:
    # API limits for different operations
    _UPSERT_LIMIT = 1000
    _DELETE_LIMIT = 1000
    _RETRIEVE_LIMIT = 1000

    # Thread pool executor concurrency limits
    _WRITE_WORKERS = 5
    _DELETE_WORKERS = 3
    _RETRIEVE_WORKERS = 10

    def __init__(self, config: PygenClientConfig) -> None:
        """Initialize the Pygen client.

        Args:
            config: Configuration for the client including URL, project, and credentials.
        """
        self._config = config
        self._http_client = HTTPClient(config)

        # Create thread pool executors for different operations
        self._write_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._WRITE_WORKERS, thread_name_prefix="pygen-write"
        )
        self._delete_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._DELETE_WORKERS, thread_name_prefix="pygen-delete"
        )
        self._retrieve_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._RETRIEVE_WORKERS, thread_name_prefix="pygen-retrieve"
        )

    def __enter__(self) -> "InstanceClient":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: object | None) -> None:
        """Exit context manager and shutdown executors."""
        self._write_executor.shutdown(wait=True)
        self._delete_executor.shutdown(wait=True)
        self._retrieve_executor.shutdown(wait=True)

    def upsert(
        self,
        items: InstanceWrite | Sequence[InstanceWrite],
        mode: Literal["replace", "update", "apply"] = "apply",
        skip_on_version_conflict: bool = False,
        auto_create_start_nodes: bool = True,
        auto_create_end_nodes: bool = True,
    ) -> InstanceResult:
        """Create or update instances.

        Args:
            items: InstanceWrite or list of InstanceWrite objects to upsert.
            mode: Upsert mode - "replace", "update", or "apply". Default is "apply".
                - "replace": Replaces the entire instance with the provided data.
                - "update": Will first retrieve the existing instances, and then merge all listable
                    properties (like lists and dicts) before upserting.
                - "apply": Applies only the provided changes to the existing instance, leaving existing
                    properties intact.
            skip_on_version_conflict: If existingVersion is specified on any of the nodes/edges in the input,
                the default behaviour is that the entire ingestion will fail when version conflicts occur.
                If skipOnVersionConflict is set to true, items with version conflicts will be skipped instead.
                If no version is specified for nodes/edges, it will do the write directly.
            auto_create_start_nodes: Auto-create start nodes for edges if they don't exist. Default is True.
            auto_create_end_nodes: Auto-create end nodes for edges if they don't exist. Default is True.

        Returns:
            InstanceResult containing details of the upsert operation.
        """
        # Normalize input to list
        item_list = [items] if isinstance(items, InstanceWrite) else list(items)

        if not item_list:
            return InstanceResult(created=[], updated=[], unchanged=[], deleted=[])

        # Handle different modes
        if mode == "update":
            # For update mode, we need to first retrieve existing instances
            # This is not implemented yet, but we'll raise a clear error
            raise NotImplementedError("Update mode is not yet implemented")

        # For replace and apply modes, we can directly call the API
        all_created: list[InstanceId] = []
        all_updated: list[InstanceId] = []
        all_unchanged: list[InstanceId] = []

        # Chunk items and submit to thread pool
        futures = []
        for chunk in chunker_sequence(item_list, self._UPSERT_LIMIT):
            future = self._write_executor.submit(
                self._upsert_chunk,
                chunk,
                mode,
                skip_on_version_conflict,
                auto_create_start_nodes,
                auto_create_end_nodes,
            )
            futures.append(future)

        # Collect results
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            all_created.extend(result.created)
            all_updated.extend(result.updated)
            all_unchanged.extend(result.unchanged)

        return InstanceResult(
            created=all_created,
            updated=all_updated,
            unchanged=all_unchanged,
            deleted=[],
        )

    def _upsert_chunk(
        self,
        items: list[InstanceWrite],
        mode: Literal["replace", "apply"],
        skip_on_version_conflict: bool,
        auto_create_start_nodes: bool,
        auto_create_end_nodes: bool,
    ) -> InstanceResult:
        """Upsert a chunk of instances via the CDF API.

        Args:
            items: List of InstanceWrite objects to upsert (max 1000).
            mode: Upsert mode - "replace" or "apply".
            skip_on_version_conflict: Whether to skip items with version conflicts.
            auto_create_start_nodes: Auto-create start nodes for edges.
            auto_create_end_nodes: Auto-create end nodes for edges.

        Returns:
            InstanceResult containing the results of the upsert operation.
        """
        # Serialize items to CDF API format
        serialized_items = [item.dump(camel_case=True, format="instance") for item in items]

        # Build request body
        body: dict[str, JsonValue] = {
            "items": serialized_items,  # type: ignore[dict-item]
            "replace": mode == "replace",
            "skipOnVersionConflict": skip_on_version_conflict,
            "autoCreateStartNodes": auto_create_start_nodes,
            "autoCreateEndNodes": auto_create_end_nodes,
        }

        # Create request
        request = RequestMessage(
            endpoint_url=self._config.create_api_url("/models/instances"),
            method="POST",
            body_content=body,
        )

        # Execute request with retries
        result = self._http_client.request_with_retries(request)
        response = result.get_success_or_raise()

        # Parse response
        return self._parse_upsert_response(response.body)

    def _parse_upsert_response(self, body: str) -> InstanceResult:
        """Parse the response from the upsert API.

        Args:
            body: The response body from the API.

        Returns:
            InstanceResult containing the results.
        """
        # The CDF API returns: {"items": [...]}
        # Each item has instanceType, space, externalId, version, etc.
        data = TypeAdapter(dict[str, list[dict[str, Any]]]).validate_json(body)
        items = data.get("items", [])

        # Parse instances - for now we treat all as created/updated
        # The API doesn't distinguish, so we assume all are successful operations
        instance_ids = [
            InstanceId(
                instance_type=item["instanceType"],
                space=item["space"],
                external_id=item["externalId"],
            )
            for item in items
        ]

        # The CDF API doesn't distinguish between created and updated,
        # so we'll put everything in created for now
        return InstanceResult(
            created=instance_ids,
            updated=[],
            unchanged=[],
            deleted=[],
        )

    def delete(
        self,
        items: str | InstanceId | InstanceWrite | Sequence[str | InstanceWrite | InstanceId],
        space: str | None = None,
    ) -> InstanceResult:
        """Delete instances.

        Args:
            items: Instance identifiers to delete. Can be a single identifier or a list.
                If providing strings, you must also provide the space parameter.
            space: Optional space identifier if items are provided as strings.

        Returns:
            InstanceResult containing details of the delete operation.
        """
        # Normalize input to list
        if isinstance(items, str | InstanceId | InstanceWrite | InstanceModel):
            item_list = [items]
        else:
            item_list = list(items)

        if not item_list:
            return InstanceResult(created=[], updated=[], unchanged=[], deleted=[])

        # Convert to InstanceId objects
        instance_ids: list[InstanceId] = []
        for item in item_list:
            if isinstance(item, str):
                if space is None:
                    raise ValueError("space parameter is required when deleting by external_id string")
                instance_ids.append(
                    InstanceId(
                        instance_type="node",  # Default to node
                        space=space,
                        external_id=item,
                    )
                )
            elif isinstance(item, InstanceId):
                instance_ids.append(item)
            elif isinstance(item, InstanceWrite | InstanceModel):
                instance_ids.append(
                    InstanceId(
                        instance_type=item.instance_type,
                        space=item.space,
                        external_id=item.external_id,
                    )
                )

        # Chunk items and submit to thread pool
        all_deleted: list[InstanceId] = []
        futures = []
        for chunk in chunker_sequence(instance_ids, self._DELETE_LIMIT):
            future = self._delete_executor.submit(self._delete_chunk, chunk)
            futures.append(future)

        # Collect results
        for future in concurrent.futures.as_completed(futures):
            deleted_ids = future.result()
            all_deleted.extend(deleted_ids)

        return InstanceResult(
            created=[],
            updated=[],
            unchanged=[],
            deleted=all_deleted,
        )

    def _delete_chunk(self, items: list[InstanceId]) -> list[InstanceId]:
        """Delete a chunk of instances via the CDF API.

        Args:
            items: List of InstanceId objects to delete (max 1000).

        Returns:
            List of deleted InstanceId objects.
        """
        # Serialize items to CDF API format
        serialized_items = [item.dump(camel_case=True, format="model") for item in items]

        # Build request body
        body: dict[str, JsonValue] = {
            "items": serialized_items,  # type: ignore[dict-item]
        }

        # Create request
        request = RequestMessage(
            endpoint_url=self._config.create_api_url("/models/instances/delete"),
            method="POST",
            body_content=body,
        )

        # Execute request with retries
        result = self._http_client.request_with_retries(request)
        response = result.get_success_or_raise()

        # Parse response
        return self._parse_delete_response(response.body, items)

    def _parse_delete_response(self, body: str, requested_items: list[InstanceId]) -> list[InstanceId]:
        """Parse the response from the delete API.

        Args:
            body: The response body from the API.
            requested_items: The items that were requested to be deleted.

        Returns:
            List of deleted InstanceId objects.
        """
        # The CDF API returns: {"items": [...]} with the deleted instances
        data = TypeAdapter(dict[str, list[dict[str, Any]]]).validate_json(body)
        items = data.get("items", [])

        # Parse deleted instances
        deleted_ids = [
            InstanceId(
                instance_type=item["instanceType"],
                space=item["space"],
                external_id=item["externalId"],
            )
            for item in items
        ]

        return deleted_ids
