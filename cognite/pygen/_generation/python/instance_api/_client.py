import concurrent.futures
from collections.abc import Sequence
from typing import Literal

from pydantic import BaseModel, JsonValue, TypeAdapter

from cognite.pygen._client import PygenClientConfig
from cognite.pygen._generation.python.instance_api.http_client import HTTPClient, RequestMessage
from cognite.pygen._utils.collection import chunker_sequence

from ._instance import InstanceId, InstanceModel, InstanceResult, InstanceResultItem, InstanceWrite


class DeleteResult(BaseModel, populate_by_name=True):
    """Result from delete operation."""

    items: list[InstanceId]


class InstanceClient:
    # API limits for different operations
    _UPSERT_LIMIT = 1000
    _DELETE_LIMIT = 1000
    _RETRIEVE_LIMIT = 1000
    _ENDPOINT = "/models/instances"

    def __init__(
        self,
        config: PygenClientConfig,
        write_workers: int = 5,
        delete_workers: int = 3,
        retrieve_workers: int = 10,
    ) -> None:
        """Initialize the Pygen client.

        Args:
            config: Configuration for the client including URL, project, and credentials.
            write_workers: Number of concurrent workers for write operations. Default is 5.
            delete_workers: Number of concurrent workers for delete operations. Default is 3.
            retrieve_workers: Number of concurrent workers for retrieve operations. Default is 10.
        """
        self._config = config
        self._http_client = HTTPClient(config)

        # Create thread pool executors for different operations
        self._write_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=write_workers, thread_name_prefix="pygen-write"
        )
        self._delete_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=delete_workers, thread_name_prefix="pygen-delete"
        )
        self._retrieve_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=retrieve_workers, thread_name_prefix="pygen-retrieve"
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

        Returns:
            InstanceResult containing details of the upsert operation.
        """
        # Normalize input to list
        item_list = [items] if isinstance(items, InstanceWrite) else list(items)

        if not item_list:
            return InstanceResult()

        # Handle different modes
        if mode == "update":
            # For update mode, we need to first retrieve existing instances
            # This is not implemented yet, but we'll raise a clear error
            raise NotImplementedError("Update mode is not yet implemented")

        # For replace and apply modes, we can directly call the API
        result = InstanceResult()

        # Chunk items and submit to thread pool
        futures = []
        for chunk in chunker_sequence(item_list, self._UPSERT_LIMIT):
            future = self._write_executor.submit(
                self._upsert_chunk,
                chunk,
                mode,
                skip_on_version_conflict,
            )
            futures.append(future)

        # Collect results
        for future in concurrent.futures.as_completed(futures):
            chunk_result = future.result()
            result.extend(chunk_result)

        return result

    def _upsert_chunk(
        self,
        items: list[InstanceWrite],
        mode: Literal["replace", "apply"],
        skip_on_version_conflict: bool,
    ) -> InstanceResult:
        """Upsert a chunk of instances via the CDF API.

        Args:
            items: List of InstanceWrite objects to upsert (max 1000).
            mode: Upsert mode - "replace" or "apply".
            skip_on_version_conflict: Whether to skip items with version conflicts.

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
        }

        # Create request
        request = RequestMessage(
            endpoint_url=self._config.create_api_url(self._ENDPOINT),
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
        # Each item has instanceType, space, externalId, version, wasModified, etc.
        data = TypeAdapter(dict[str, list[InstanceResultItem]]).validate_json(body)
        items = data.get("items", [])

        # Separate items based on wasModified flag
        created_or_updated = [item for item in items if item.was_modified]
        unchanged = [item for item in items if not item.was_modified]

        return InstanceResult(
            created=created_or_updated,
            updated=[],
            unchanged=unchanged,
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
            return InstanceResult()

        # Convert to InstanceId objects
        instance_ids = [self._to_instance_id(item, space) for item in item_list]

        # Chunk items and submit to thread pool
        result = InstanceResult()
        futures = []
        for chunk in chunker_sequence(instance_ids, self._DELETE_LIMIT):
            future = self._delete_executor.submit(self._delete_chunk, chunk)
            futures.append(future)

        # Collect results
        for future in concurrent.futures.as_completed(futures):
            deleted_ids = future.result()
            result.deleted.extend(deleted_ids)

        return result

    def _to_instance_id(self, item: str | InstanceId | InstanceWrite | InstanceModel, space: str | None) -> InstanceId:
        """Convert various input types to InstanceId.

        Args:
            item: The item to convert.
            space: Optional space identifier if item is a string.

        Returns:
            InstanceId object.

        Raises:
            ValueError: If space is None when item is a string.
        """
        if isinstance(item, str):
            if space is None:
                raise ValueError("space parameter is required when deleting by external_id string")
            return InstanceId(
                instance_type="node",  # Default to node
                space=space,
                external_id=item,
            )
        elif isinstance(item, InstanceId):
            return item
        elif isinstance(item, InstanceWrite | InstanceModel):
            return InstanceId(
                instance_type=item.instance_type,
                space=item.space,
                external_id=item.external_id,
            )
        else:
            raise TypeError(f"Unsupported type for item: {type(item)}")

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
        delete_result = TypeAdapter(DeleteResult).validate_json(body)
        return delete_result.items
