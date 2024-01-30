from __future__ import annotations

from pathlib import Path
from typing import Sequence

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.credentials import OAuthClientCredentials

from ._api.scenario_instance import ScenarioInstanceAPI
from ._api._core import SequenceNotStr
from .data_classes._core import DEFAULT_INSTANCE_SPACE
from . import data_classes


class ScenarioInstanceClient:
    """
    ScenarioInstanceClient

    Generated with:
        pygen = 0.99.0
        cognite-sdk = 7.13.6
        pydantic = 2.5.3

    Data Model:
        space: IntegrationTestsImmutable
        externalId: ScenarioInstance
        version: 1
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        # The client name is used for aggregated logging of Pygen Usage
        client.config.client_name = "CognitePygen:0.99.0"

        view_by_read_class = {
            data_classes.ScenarioInstance: dm.ViewId("IntegrationTestsImmutable", "ScenarioInstance", "ee2b79fd98b5bb"),
        }
        self._view_by_read_class = view_by_read_class
        self._client = client

        self.scenario_instance = ScenarioInstanceAPI(client, view_by_read_class)

    def apply(
        self,
        items: data_classes.DomainModelApply | Sequence[data_classes.DomainModelApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> data_classes.ResourcesApplyResult:
        """Add or update (upsert) items.

        Args:
            items: One or more instances of the pygen generated data classes.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method will, by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        """
        if isinstance(items, data_classes.DomainModelApply):
            instances = items.to_instances_apply(self._view_by_read_class, write_none)
        else:
            instances = [item.to_instances_apply(self._view_by_read_class, write_none) for item in items]
        result = self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )
        time_series = []
        if instances.time_series:
            time_series = self._client.time_series.upsert(instances.time_series, mode="patch")

        return data_classes.ResourcesApplyResult(result.nodes, result.edges, TimeSeriesList(time_series))

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more items.

        Args:
            external_id: External id of the item(s) to delete.
            space: The space where all the item(s) are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete item by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.delete("my_node_external_id")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> ScenarioInstanceClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> ScenarioInstanceClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)

    def _repr_html_(self) -> str:
        return """<strong>ScenarioInstanceClient</strong> generated from data model ("IntegrationTestsImmutable", "ScenarioInstance", "1")<br />
with the following APIs available<br />
&nbsp;&nbsp;&nbsp;&nbsp;.scenario_instance<br />
<br />
and with the methods:<br />
&nbsp;&nbsp;&nbsp;&nbsp;.apply - Create or update any instance.<br />
"""
