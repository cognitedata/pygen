from collections.abc import Sequence
from typing import Literal

from cognite.pygen._client import PygenClientConfig
from cognite.pygen._generation.python.instance_api.http_client import HTTPClient

from ._instance import InstanceId, InstanceResult, InstanceWrite


class InstanceClient:
    def __init__(self, config: PygenClientConfig) -> None:
        """Initialize the Pygen client.

        Args:
            config: Configuration for the client including URL, project, and credentials.
        """
        self._config = config
        self._http_client = HTTPClient(config)

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
                - "update": Will first retrieve th existing instances, and then merge all listable
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
        raise NotImplementedError

    def delete(
        self,
        items: str | InstanceId | InstanceWrite | Sequence[str | InstanceWrite | InstanceId],
        space: str | None = None,
    ) -> InstanceResult:
        """Delete instances.

        Args:
            items: Instance identifiers to delete. Can be a single identifier or a list.
            space: Optional space identifier if instances are in a specific space.
        Returns:
            InstanceResult containing details of the delete operation.
        """
        raise NotImplementedError
