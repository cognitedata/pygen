import concurrent.futures
from collections.abc import Callable, Sequence
from typing import Literal, TypeVar

from pydantic import JsonValue

from cognite.pygen._client import PygenClientConfig
from cognite.pygen._generation.python.instance_api import InstanceId
from cognite.pygen._generation.python.instance_api.exceptions import MultiRequestError
from cognite.pygen._generation.python.instance_api.http_client import (
    FailedRequest,
    FailedResponse,
    HTTPClient,
    HTTPResult,
    RequestMessage,
    SuccessResponse,
)
from cognite.pygen._generation.python.instance_api.models.instance import InstanceModel, InstanceWrite
from cognite.pygen._generation.python.instance_api.models.responses import ApplyResponse, DeleteResponse, InstanceResult
from cognite.pygen._utils.collection import chunker_sequence

T = TypeVar("T")


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

    @classmethod
    def _execute_in_parallel(
        cls,
        items: list[T],
        chunk_size: int,
        executor: concurrent.futures.ThreadPoolExecutor,
        task_fn: Callable[[list[T]], HTTPResult],
    ) -> list[HTTPResult]:
        """Execute a task function in parallel on chunked items.

        Args:
            items: List of items to process.
            chunk_size: Maximum size of each chunk.
            executor: Thread pool executor to use.
            task_fn: Function to execute on each chunk.

        Returns:
            List of HTTPResult objects from all tasks.
        """
        futures = [executor.submit(task_fn, chunk) for chunk in chunker_sequence(items, chunk_size)]
        return [future.result() for future in concurrent.futures.as_completed(futures)]

    @classmethod
    def _collect_results(
        cls,
        results: list[HTTPResult],
        parse_success: Callable[[str], InstanceResult],
    ) -> InstanceResult:
        """Collect results from HTTP responses, raising on failures.

        Args:
            results: List of HTTPResult objects.
            parse_success: Function to parse a successful response body into InstanceResult.

        Returns:
            Combined InstanceResult from all successful operations.

        Raises:
            MultiRequestError: If any of the HTTPResults indicate a failure.
        """
        combined_result = InstanceResult()
        failed_responses: list[FailedResponse] = []
        failed_requests: list[FailedRequest] = []

        for result in results:
            if isinstance(result, SuccessResponse):
                combined_result.extend(parse_success(result.body))
            elif isinstance(result, FailedResponse):
                failed_responses.append(result)
            elif isinstance(result, FailedRequest):
                failed_requests.append(result)

        if failed_responses or failed_requests:
            raise MultiRequestError(failed_responses, failed_requests, combined_result)

        return combined_result

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
        item_list = [items] if isinstance(items, InstanceWrite) else list(items)

        if not item_list:
            return InstanceResult()

        if mode == "update":
            # For update mode, we need to first retrieve existing instances
            # This is not implemented yet, but we'll raise a clear error
            raise NotImplementedError("Update mode is not yet implemented")

        def upsert_chunk(chunk: list[InstanceWrite]) -> HTTPResult:
            return self._upsert_chunk(chunk, mode, skip_on_version_conflict)

        http_results = self._execute_in_parallel(item_list, self._UPSERT_LIMIT, self._write_executor, upsert_chunk)
        return self._collect_results(http_results, self._parse_upsert_response)

    def _upsert_chunk(
        self,
        items: list[InstanceWrite],
        mode: Literal["replace", "apply"],
        skip_on_version_conflict: bool,
    ) -> HTTPResult:
        """Upsert a chunk of instances via the CDF API.

        Args:
            items: List of InstanceWrite objects to upsert (max 1000).
            mode: Upsert mode - "replace" or "apply".
            skip_on_version_conflict: Whether to skip items with version conflicts.

        Returns:
            InstanceResult containing the results of the upsert operation.
        """
        serialized_items = [item.dump(camel_case=True, format="instance") for item in items]

        body: dict[str, JsonValue] = {
            "items": serialized_items,  # type: ignore[dict-item]
            "replace": mode == "replace",
            "skipOnVersionConflict": skip_on_version_conflict,
        }

        request = RequestMessage(
            endpoint_url=self._config.create_api_url(self._ENDPOINT),
            method="POST",
            body_content=body,
        )
        return self._http_client.request_with_retries(request)

    @staticmethod
    def _parse_upsert_response(body: str) -> InstanceResult:
        """Parse the response from the upsert API.

        Args:
            body: The response body from the API.

        Returns:
            InstanceResult containing the results.
        """
        response = ApplyResponse.model_validate_json(body)
        result = InstanceResult(deleted=response.deleted)
        for item in response.items:
            if not item.was_modified:
                result.unchanged.append(item)
                continue
            if item.created_time == item.last_updated_time:
                result.created.append(item)
            else:
                result.updated.append(item)
        return result

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

        instance_ids = [self._to_instance_id(item, space) for item in item_list]

        http_results = self._execute_in_parallel(
            instance_ids, self._DELETE_LIMIT, self._delete_executor, self._delete_chunk
        )
        return self._collect_results(http_results, self._parse_delete_response)

    @staticmethod
    def _to_instance_id(item: str | InstanceId | InstanceWrite | InstanceModel, space: str | None) -> InstanceId:
        """Convert various input types to InstanceId.

        Args:
            item: The item to convert.
            space: Optional space identifier if item is a string.

        Returns:
            InstanceId object.

        Raises:
            ValueError: If space is None when item is a string.
        """
        if isinstance(item, InstanceId):
            return item
        if isinstance(item, InstanceWrite | InstanceModel):
            return InstanceId(
                instance_type=item.instance_type,
                space=item.space,
                external_id=item.external_id,
            )
        if isinstance(item, str):
            if space is None:
                raise ValueError("space parameter is required when deleting by external_id string")
            return InstanceId(
                instance_type="node",  # Default to node
                space=space,
                external_id=item,
            )
        raise TypeError(f"Unsupported type for item: {type(item)}")

    def _delete_chunk(self, items: list[InstanceId]) -> HTTPResult:
        """Delete a chunk of instances via the CDF API.

        Args:
            items: List of InstanceId objects to delete (max 1000).

        Returns:
            List of deleted InstanceId objects.
        """
        serialized_items = [item.dump(camel_case=True, format="model") for item in items]

        body: dict[str, JsonValue] = {
            "items": serialized_items,  # type: ignore[dict-item]
        }
        request = RequestMessage(
            endpoint_url=self._config.create_api_url("/models/instances/delete"),
            method="POST",
            body_content=body,
        )

        return self._http_client.request_with_retries(request)

    @staticmethod
    def _parse_delete_response(body: str) -> InstanceResult:
        """Parse the response from the delete API.

        Args:
            body: The response body from the API.

        Returns:
            InstanceResult containing the deleted items.
        """
        deleted = DeleteResponse.model_validate_json(body).items
        return InstanceResult(deleted=deleted)
