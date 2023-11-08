from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    WellboreTrajectoryData,
    WellboreTrajectoryDataApply,
    WellboreTrajectoryDataList,
    WellboreTrajectoryDataApplyList,
    WellboreTrajectoryDataFields,
    WellboreTrajectoryDataTextFields,
    DomainModelApply,
)
from osdu_wells_pydantic_v1.client.data_classes._wellbore_trajectory_data import (
    _WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD,
)


class WellboreTrajectoryDataArtefactsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more artefacts edges by id(s) of a wellbore trajectory datum.

        Args:
            external_id: External id or list of external ids source wellbore trajectory datum.
            space: The space where all the artefact edges are located.

        Returns:
            The requested artefact edges.

        Examples:

            Retrieve artefacts edge by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.artefacts.retrieve("my_artefacts")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreTrajectoryData.Artefacts"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_trajectory_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_trajectory_data)
        )

    def list(
        self,
        wellbore_trajectory_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List artefacts edges of a wellbore trajectory datum.

        Args:
            wellbore_trajectory_datum_id: ID of the source wellbore trajectory datum.
            limit: Maximum number of artefact edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the artefact edges are located.

        Returns:
            The requested artefact edges.

        Examples:

            List 5 artefacts edges connected to "my_wellbore_trajectory_datum":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.artefacts.list("my_wellbore_trajectory_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreTrajectoryData.Artefacts"},
            )
        ]
        if wellbore_trajectory_datum_id:
            wellbore_trajectory_datum_ids = (
                wellbore_trajectory_datum_id
                if isinstance(wellbore_trajectory_datum_id, list)
                else [wellbore_trajectory_datum_id]
            )
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_trajectory_datum_ids
                ],
            )
            filters.append(is_wellbore_trajectory_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreTrajectoryDataAvailableTrajectoryStationPropertiesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more available_trajectory_station_properties edges by id(s) of a wellbore trajectory datum.

        Args:
            external_id: External id or list of external ids source wellbore trajectory datum.
            space: The space where all the available trajectory station property edges are located.

        Returns:
            The requested available trajectory station property edges.

        Examples:

            Retrieve available_trajectory_station_properties edge by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.available_trajectory_station_properties.retrieve("my_available_trajectory_station_properties")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {
                "space": "IntegrationTestsImmutable",
                "externalId": "WellboreTrajectoryData.AvailableTrajectoryStationProperties",
            },
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_trajectory_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_trajectory_data)
        )

    def list(
        self,
        wellbore_trajectory_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List available_trajectory_station_properties edges of a wellbore trajectory datum.

        Args:
            wellbore_trajectory_datum_id: ID of the source wellbore trajectory datum.
            limit: Maximum number of available trajectory station property edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the available trajectory station property edges are located.

        Returns:
            The requested available trajectory station property edges.

        Examples:

            List 5 available_trajectory_station_properties edges connected to "my_wellbore_trajectory_datum":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.available_trajectory_station_properties.list("my_wellbore_trajectory_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {
                    "space": "IntegrationTestsImmutable",
                    "externalId": "WellboreTrajectoryData.AvailableTrajectoryStationProperties",
                },
            )
        ]
        if wellbore_trajectory_datum_id:
            wellbore_trajectory_datum_ids = (
                wellbore_trajectory_datum_id
                if isinstance(wellbore_trajectory_datum_id, list)
                else [wellbore_trajectory_datum_id]
            )
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_trajectory_datum_ids
                ],
            )
            filters.append(is_wellbore_trajectory_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreTrajectoryDataGeoContextsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more geo_contexts edges by id(s) of a wellbore trajectory datum.

        Args:
            external_id: External id or list of external ids source wellbore trajectory datum.
            space: The space where all the geo context edges are located.

        Returns:
            The requested geo context edges.

        Examples:

            Retrieve geo_contexts edge by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.geo_contexts.retrieve("my_geo_contexts")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreTrajectoryData.GeoContexts"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_trajectory_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_trajectory_data)
        )

    def list(
        self,
        wellbore_trajectory_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List geo_contexts edges of a wellbore trajectory datum.

        Args:
            wellbore_trajectory_datum_id: ID of the source wellbore trajectory datum.
            limit: Maximum number of geo context edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the geo context edges are located.

        Returns:
            The requested geo context edges.

        Examples:

            List 5 geo_contexts edges connected to "my_wellbore_trajectory_datum":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.geo_contexts.list("my_wellbore_trajectory_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreTrajectoryData.GeoContexts"},
            )
        ]
        if wellbore_trajectory_datum_id:
            wellbore_trajectory_datum_ids = (
                wellbore_trajectory_datum_id
                if isinstance(wellbore_trajectory_datum_id, list)
                else [wellbore_trajectory_datum_id]
            )
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_trajectory_datum_ids
                ],
            )
            filters.append(is_wellbore_trajectory_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreTrajectoryDataLineageAssertionsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more lineage_assertions edges by id(s) of a wellbore trajectory datum.

        Args:
            external_id: External id or list of external ids source wellbore trajectory datum.
            space: The space where all the lineage assertion edges are located.

        Returns:
            The requested lineage assertion edges.

        Examples:

            Retrieve lineage_assertions edge by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.lineage_assertions.retrieve("my_lineage_assertions")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreTrajectoryData.LineageAssertions"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_trajectory_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_trajectory_data)
        )

    def list(
        self,
        wellbore_trajectory_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List lineage_assertions edges of a wellbore trajectory datum.

        Args:
            wellbore_trajectory_datum_id: ID of the source wellbore trajectory datum.
            limit: Maximum number of lineage assertion edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the lineage assertion edges are located.

        Returns:
            The requested lineage assertion edges.

        Examples:

            List 5 lineage_assertions edges connected to "my_wellbore_trajectory_datum":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.lineage_assertions.list("my_wellbore_trajectory_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreTrajectoryData.LineageAssertions"},
            )
        ]
        if wellbore_trajectory_datum_id:
            wellbore_trajectory_datum_ids = (
                wellbore_trajectory_datum_id
                if isinstance(wellbore_trajectory_datum_id, list)
                else [wellbore_trajectory_datum_id]
            )
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_trajectory_datum_ids
                ],
            )
            filters.append(is_wellbore_trajectory_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreTrajectoryDataNameAliasesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more name_aliases edges by id(s) of a wellbore trajectory datum.

        Args:
            external_id: External id or list of external ids source wellbore trajectory datum.
            space: The space where all the name alias edges are located.

        Returns:
            The requested name alias edges.

        Examples:

            Retrieve name_aliases edge by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.name_aliases.retrieve("my_name_aliases")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreTrajectoryData.NameAliases"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_trajectory_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_trajectory_data)
        )

    def list(
        self,
        wellbore_trajectory_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List name_aliases edges of a wellbore trajectory datum.

        Args:
            wellbore_trajectory_datum_id: ID of the source wellbore trajectory datum.
            limit: Maximum number of name alias edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the name alias edges are located.

        Returns:
            The requested name alias edges.

        Examples:

            List 5 name_aliases edges connected to "my_wellbore_trajectory_datum":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.name_aliases.list("my_wellbore_trajectory_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreTrajectoryData.NameAliases"},
            )
        ]
        if wellbore_trajectory_datum_id:
            wellbore_trajectory_datum_ids = (
                wellbore_trajectory_datum_id
                if isinstance(wellbore_trajectory_datum_id, list)
                else [wellbore_trajectory_datum_id]
            )
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_trajectory_datum_ids
                ],
            )
            filters.append(is_wellbore_trajectory_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreTrajectoryDataTechnicalAssurancesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more technical_assurances edges by id(s) of a wellbore trajectory datum.

        Args:
            external_id: External id or list of external ids source wellbore trajectory datum.
            space: The space where all the technical assurance edges are located.

        Returns:
            The requested technical assurance edges.

        Examples:

            Retrieve technical_assurances edge by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.technical_assurances.retrieve("my_technical_assurances")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreTrajectoryData.TechnicalAssurances"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_trajectory_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_trajectory_data)
        )

    def list(
        self,
        wellbore_trajectory_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List technical_assurances edges of a wellbore trajectory datum.

        Args:
            wellbore_trajectory_datum_id: ID of the source wellbore trajectory datum.
            limit: Maximum number of technical assurance edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the technical assurance edges are located.

        Returns:
            The requested technical assurance edges.

        Examples:

            List 5 technical_assurances edges connected to "my_wellbore_trajectory_datum":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.technical_assurances.list("my_wellbore_trajectory_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreTrajectoryData.TechnicalAssurances"},
            )
        ]
        if wellbore_trajectory_datum_id:
            wellbore_trajectory_datum_ids = (
                wellbore_trajectory_datum_id
                if isinstance(wellbore_trajectory_datum_id, list)
                else [wellbore_trajectory_datum_id]
            )
            is_wellbore_trajectory_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_trajectory_datum_ids
                ],
            )
            filters.append(is_wellbore_trajectory_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreTrajectoryDataAPI(
    TypeAPI[WellboreTrajectoryData, WellboreTrajectoryDataApply, WellboreTrajectoryDataList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WellboreTrajectoryDataApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WellboreTrajectoryData,
            class_apply_type=WellboreTrajectoryDataApply,
            class_list=WellboreTrajectoryDataList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.artefacts = WellboreTrajectoryDataArtefactsAPI(client)
        self.available_trajectory_station_properties = WellboreTrajectoryDataAvailableTrajectoryStationPropertiesAPI(
            client
        )
        self.geo_contexts = WellboreTrajectoryDataGeoContextsAPI(client)
        self.lineage_assertions = WellboreTrajectoryDataLineageAssertionsAPI(client)
        self.name_aliases = WellboreTrajectoryDataNameAliasesAPI(client)
        self.technical_assurances = WellboreTrajectoryDataTechnicalAssurancesAPI(client)

    def apply(
        self,
        wellbore_trajectory_datum: WellboreTrajectoryDataApply | Sequence[WellboreTrajectoryDataApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) wellbore trajectory data.

        Note: This method iterates through all nodes linked to wellbore_trajectory_datum and create them including the edges
        between the nodes. For example, if any of `artefacts`, `available_trajectory_station_properties`, `geo_contexts`, `lineage_assertions`, `name_aliases` or `technical_assurances` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            wellbore_trajectory_datum: Wellbore trajectory datum or sequence of wellbore trajectory data to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new wellbore_trajectory_datum:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import WellboreTrajectoryDataApply
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = WellboreTrajectoryDataApply(external_id="my_wellbore_trajectory_datum", ...)
                >>> result = client.wellbore_trajectory_data.apply(wellbore_trajectory_datum)

        """
        if isinstance(wellbore_trajectory_datum, WellboreTrajectoryDataApply):
            instances = wellbore_trajectory_datum.to_instances_apply(self._view_by_write_class)
        else:
            instances = WellboreTrajectoryDataApplyList(wellbore_trajectory_datum).to_instances_apply(
                self._view_by_write_class
            )
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more wellbore trajectory datum.

        Args:
            external_id: External id of the wellbore trajectory datum to delete.
            space: The space where all the wellbore trajectory datum are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete wellbore_trajectory_datum by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.wellbore_trajectory_data.delete("my_wellbore_trajectory_datum")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WellboreTrajectoryData:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WellboreTrajectoryDataList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> WellboreTrajectoryData | WellboreTrajectoryDataList:
        """Retrieve one or more wellbore trajectory data by id(s).

        Args:
            external_id: External id or list of external ids of the wellbore trajectory data.
            space: The space where all the wellbore trajectory data are located.

        Returns:
            The requested wellbore trajectory data.

        Examples:

            Retrieve wellbore_trajectory_datum by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.retrieve("my_wellbore_trajectory_datum")

        """
        if isinstance(external_id, str):
            wellbore_trajectory_datum = self._retrieve((space, external_id))

            artefact_edges = self.artefacts.retrieve(external_id, space=space)
            wellbore_trajectory_datum.artefacts = [edge.end_node.external_id for edge in artefact_edges]
            available_trajectory_station_property_edges = self.available_trajectory_station_properties.retrieve(
                external_id, space=space
            )
            wellbore_trajectory_datum.available_trajectory_station_properties = [
                edge.end_node.external_id for edge in available_trajectory_station_property_edges
            ]
            geo_context_edges = self.geo_contexts.retrieve(external_id, space=space)
            wellbore_trajectory_datum.geo_contexts = [edge.end_node.external_id for edge in geo_context_edges]
            lineage_assertion_edges = self.lineage_assertions.retrieve(external_id, space=space)
            wellbore_trajectory_datum.lineage_assertions = [
                edge.end_node.external_id for edge in lineage_assertion_edges
            ]
            name_alias_edges = self.name_aliases.retrieve(external_id, space=space)
            wellbore_trajectory_datum.name_aliases = [edge.end_node.external_id for edge in name_alias_edges]
            technical_assurance_edges = self.technical_assurances.retrieve(external_id, space=space)
            wellbore_trajectory_datum.technical_assurances = [
                edge.end_node.external_id for edge in technical_assurance_edges
            ]

            return wellbore_trajectory_datum
        else:
            wellbore_trajectory_data = self._retrieve([(space, ext_id) for ext_id in external_id])

            artefact_edges = self.artefacts.retrieve(wellbore_trajectory_data.as_node_ids())
            self._set_artefacts(wellbore_trajectory_data, artefact_edges)
            available_trajectory_station_property_edges = self.available_trajectory_station_properties.retrieve(
                wellbore_trajectory_data.as_node_ids()
            )
            self._set_available_trajectory_station_properties(
                wellbore_trajectory_data, available_trajectory_station_property_edges
            )
            geo_context_edges = self.geo_contexts.retrieve(wellbore_trajectory_data.as_node_ids())
            self._set_geo_contexts(wellbore_trajectory_data, geo_context_edges)
            lineage_assertion_edges = self.lineage_assertions.retrieve(wellbore_trajectory_data.as_node_ids())
            self._set_lineage_assertions(wellbore_trajectory_data, lineage_assertion_edges)
            name_alias_edges = self.name_aliases.retrieve(wellbore_trajectory_data.as_node_ids())
            self._set_name_aliases(wellbore_trajectory_data, name_alias_edges)
            technical_assurance_edges = self.technical_assurances.retrieve(wellbore_trajectory_data.as_node_ids())
            self._set_technical_assurances(wellbore_trajectory_data, technical_assurance_edges)

            return wellbore_trajectory_data

    def search(
        self,
        query: str,
        properties: WellboreTrajectoryDataTextFields | Sequence[WellboreTrajectoryDataTextFields] | None = None,
        acquisition_date: str | list[str] | None = None,
        acquisition_date_prefix: str | None = None,
        acquisition_remark: str | list[str] | None = None,
        acquisition_remark_prefix: str | None = None,
        active_indicator: bool | None = None,
        applied_operations_date_time: str | list[str] | None = None,
        applied_operations_date_time_prefix: str | None = None,
        applied_operations_remarks: str | list[str] | None = None,
        applied_operations_remarks_prefix: str | None = None,
        applied_operations_user: str | list[str] | None = None,
        applied_operations_user_prefix: str | None = None,
        azimuth_reference_type: str | list[str] | None = None,
        azimuth_reference_type_prefix: str | None = None,
        min_base_depth_measured_depth: int | None = None,
        max_base_depth_measured_depth: int | None = None,
        calculation_method_type: str | list[str] | None = None,
        calculation_method_type_prefix: str | None = None,
        company_id: str | list[str] | None = None,
        company_id_prefix: str | None = None,
        creation_date_time: str | list[str] | None = None,
        creation_date_time_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        end_date_time: str | list[str] | None = None,
        end_date_time_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        min_extrapolated_measured_depth: int | None = None,
        max_extrapolated_measured_depth: int | None = None,
        extrapolated_measured_depth_remark: str | list[str] | None = None,
        extrapolated_measured_depth_remark_prefix: str | None = None,
        geographic_crsid: str | list[str] | None = None,
        geographic_crsid_prefix: str | None = None,
        is_discoverable: bool | None = None,
        is_extended_load: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        projected_crsid: str | list[str] | None = None,
        projected_crsid_prefix: str | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        service_company_id: str | list[str] | None = None,
        service_company_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        spatial_point: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        start_date_time: str | list[str] | None = None,
        start_date_time_prefix: str | None = None,
        submitter_name: str | list[str] | None = None,
        submitter_name_prefix: str | None = None,
        min_surface_grid_convergence: float | None = None,
        max_surface_grid_convergence: float | None = None,
        min_surface_scale_factor: float | None = None,
        max_surface_scale_factor: float | None = None,
        survey_reference_identifier: str | list[str] | None = None,
        survey_reference_identifier_prefix: str | None = None,
        survey_tool_type_id: str | list[str] | None = None,
        survey_tool_type_id_prefix: str | None = None,
        survey_type: str | list[str] | None = None,
        survey_type_prefix: str | None = None,
        survey_version: str | list[str] | None = None,
        survey_version_prefix: str | None = None,
        min_tie_measured_depth: int | None = None,
        max_tie_measured_depth: int | None = None,
        min_tie_true_vertical_depth: int | None = None,
        max_tie_true_vertical_depth: int | None = None,
        min_top_depth_measured_depth: int | None = None,
        max_top_depth_measured_depth: int | None = None,
        min_tortuosity: float | None = None,
        max_tortuosity: float | None = None,
        vertical_measurement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        wellbore_id: str | list[str] | None = None,
        wellbore_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellboreTrajectoryDataList:
        """Search wellbore trajectory data

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            acquisition_date: The acquisition date to filter on.
            acquisition_date_prefix: The prefix of the acquisition date to filter on.
            acquisition_remark: The acquisition remark to filter on.
            acquisition_remark_prefix: The prefix of the acquisition remark to filter on.
            active_indicator: The active indicator to filter on.
            applied_operations_date_time: The applied operations date time to filter on.
            applied_operations_date_time_prefix: The prefix of the applied operations date time to filter on.
            applied_operations_remarks: The applied operations remark to filter on.
            applied_operations_remarks_prefix: The prefix of the applied operations remark to filter on.
            applied_operations_user: The applied operations user to filter on.
            applied_operations_user_prefix: The prefix of the applied operations user to filter on.
            azimuth_reference_type: The azimuth reference type to filter on.
            azimuth_reference_type_prefix: The prefix of the azimuth reference type to filter on.
            min_base_depth_measured_depth: The minimum value of the base depth measured depth to filter on.
            max_base_depth_measured_depth: The maximum value of the base depth measured depth to filter on.
            calculation_method_type: The calculation method type to filter on.
            calculation_method_type_prefix: The prefix of the calculation method type to filter on.
            company_id: The company id to filter on.
            company_id_prefix: The prefix of the company id to filter on.
            creation_date_time: The creation date time to filter on.
            creation_date_time_prefix: The prefix of the creation date time to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            end_date_time: The end date time to filter on.
            end_date_time_prefix: The prefix of the end date time to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            min_extrapolated_measured_depth: The minimum value of the extrapolated measured depth to filter on.
            max_extrapolated_measured_depth: The maximum value of the extrapolated measured depth to filter on.
            extrapolated_measured_depth_remark: The extrapolated measured depth remark to filter on.
            extrapolated_measured_depth_remark_prefix: The prefix of the extrapolated measured depth remark to filter on.
            geographic_crsid: The geographic crsid to filter on.
            geographic_crsid_prefix: The prefix of the geographic crsid to filter on.
            is_discoverable: The is discoverable to filter on.
            is_extended_load: The is extended load to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            projected_crsid: The projected crsid to filter on.
            projected_crsid_prefix: The prefix of the projected crsid to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            service_company_id: The service company id to filter on.
            service_company_id_prefix: The prefix of the service company id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_area: The spatial area to filter on.
            spatial_point: The spatial point to filter on.
            start_date_time: The start date time to filter on.
            start_date_time_prefix: The prefix of the start date time to filter on.
            submitter_name: The submitter name to filter on.
            submitter_name_prefix: The prefix of the submitter name to filter on.
            min_surface_grid_convergence: The minimum value of the surface grid convergence to filter on.
            max_surface_grid_convergence: The maximum value of the surface grid convergence to filter on.
            min_surface_scale_factor: The minimum value of the surface scale factor to filter on.
            max_surface_scale_factor: The maximum value of the surface scale factor to filter on.
            survey_reference_identifier: The survey reference identifier to filter on.
            survey_reference_identifier_prefix: The prefix of the survey reference identifier to filter on.
            survey_tool_type_id: The survey tool type id to filter on.
            survey_tool_type_id_prefix: The prefix of the survey tool type id to filter on.
            survey_type: The survey type to filter on.
            survey_type_prefix: The prefix of the survey type to filter on.
            survey_version: The survey version to filter on.
            survey_version_prefix: The prefix of the survey version to filter on.
            min_tie_measured_depth: The minimum value of the tie measured depth to filter on.
            max_tie_measured_depth: The maximum value of the tie measured depth to filter on.
            min_tie_true_vertical_depth: The minimum value of the tie true vertical depth to filter on.
            max_tie_true_vertical_depth: The maximum value of the tie true vertical depth to filter on.
            min_top_depth_measured_depth: The minimum value of the top depth measured depth to filter on.
            max_top_depth_measured_depth: The maximum value of the top depth measured depth to filter on.
            min_tortuosity: The minimum value of the tortuosity to filter on.
            max_tortuosity: The maximum value of the tortuosity to filter on.
            vertical_measurement: The vertical measurement to filter on.
            wellbore_id: The wellbore id to filter on.
            wellbore_id_prefix: The prefix of the wellbore id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore trajectory data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `artefacts`, `available_trajectory_station_properties`, `geo_contexts`, `lineage_assertions`, `name_aliases` or `technical_assurances` external ids for the wellbore trajectory data. Defaults to True.

        Returns:
            Search results wellbore trajectory data matching the query.

        Examples:

           Search for 'my_wellbore_trajectory_datum' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_data = client.wellbore_trajectory_data.search('my_wellbore_trajectory_datum')

        """
        filter_ = _create_filter(
            self._view_id,
            acquisition_date,
            acquisition_date_prefix,
            acquisition_remark,
            acquisition_remark_prefix,
            active_indicator,
            applied_operations_date_time,
            applied_operations_date_time_prefix,
            applied_operations_remarks,
            applied_operations_remarks_prefix,
            applied_operations_user,
            applied_operations_user_prefix,
            azimuth_reference_type,
            azimuth_reference_type_prefix,
            min_base_depth_measured_depth,
            max_base_depth_measured_depth,
            calculation_method_type,
            calculation_method_type_prefix,
            company_id,
            company_id_prefix,
            creation_date_time,
            creation_date_time_prefix,
            description,
            description_prefix,
            end_date_time,
            end_date_time_prefix,
            existence_kind,
            existence_kind_prefix,
            min_extrapolated_measured_depth,
            max_extrapolated_measured_depth,
            extrapolated_measured_depth_remark,
            extrapolated_measured_depth_remark_prefix,
            geographic_crsid,
            geographic_crsid_prefix,
            is_discoverable,
            is_extended_load,
            name,
            name_prefix,
            projected_crsid,
            projected_crsid_prefix,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            service_company_id,
            service_company_id_prefix,
            source,
            source_prefix,
            spatial_area,
            spatial_point,
            start_date_time,
            start_date_time_prefix,
            submitter_name,
            submitter_name_prefix,
            min_surface_grid_convergence,
            max_surface_grid_convergence,
            min_surface_scale_factor,
            max_surface_scale_factor,
            survey_reference_identifier,
            survey_reference_identifier_prefix,
            survey_tool_type_id,
            survey_tool_type_id_prefix,
            survey_type,
            survey_type_prefix,
            survey_version,
            survey_version_prefix,
            min_tie_measured_depth,
            max_tie_measured_depth,
            min_tie_true_vertical_depth,
            max_tie_true_vertical_depth,
            min_top_depth_measured_depth,
            max_top_depth_measured_depth,
            min_tortuosity,
            max_tortuosity,
            vertical_measurement,
            wellbore_id,
            wellbore_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellboreTrajectoryDataFields | Sequence[WellboreTrajectoryDataFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WellboreTrajectoryDataTextFields | Sequence[WellboreTrajectoryDataTextFields] | None = None,
        acquisition_date: str | list[str] | None = None,
        acquisition_date_prefix: str | None = None,
        acquisition_remark: str | list[str] | None = None,
        acquisition_remark_prefix: str | None = None,
        active_indicator: bool | None = None,
        applied_operations_date_time: str | list[str] | None = None,
        applied_operations_date_time_prefix: str | None = None,
        applied_operations_remarks: str | list[str] | None = None,
        applied_operations_remarks_prefix: str | None = None,
        applied_operations_user: str | list[str] | None = None,
        applied_operations_user_prefix: str | None = None,
        azimuth_reference_type: str | list[str] | None = None,
        azimuth_reference_type_prefix: str | None = None,
        min_base_depth_measured_depth: int | None = None,
        max_base_depth_measured_depth: int | None = None,
        calculation_method_type: str | list[str] | None = None,
        calculation_method_type_prefix: str | None = None,
        company_id: str | list[str] | None = None,
        company_id_prefix: str | None = None,
        creation_date_time: str | list[str] | None = None,
        creation_date_time_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        end_date_time: str | list[str] | None = None,
        end_date_time_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        min_extrapolated_measured_depth: int | None = None,
        max_extrapolated_measured_depth: int | None = None,
        extrapolated_measured_depth_remark: str | list[str] | None = None,
        extrapolated_measured_depth_remark_prefix: str | None = None,
        geographic_crsid: str | list[str] | None = None,
        geographic_crsid_prefix: str | None = None,
        is_discoverable: bool | None = None,
        is_extended_load: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        projected_crsid: str | list[str] | None = None,
        projected_crsid_prefix: str | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        service_company_id: str | list[str] | None = None,
        service_company_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        spatial_point: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        start_date_time: str | list[str] | None = None,
        start_date_time_prefix: str | None = None,
        submitter_name: str | list[str] | None = None,
        submitter_name_prefix: str | None = None,
        min_surface_grid_convergence: float | None = None,
        max_surface_grid_convergence: float | None = None,
        min_surface_scale_factor: float | None = None,
        max_surface_scale_factor: float | None = None,
        survey_reference_identifier: str | list[str] | None = None,
        survey_reference_identifier_prefix: str | None = None,
        survey_tool_type_id: str | list[str] | None = None,
        survey_tool_type_id_prefix: str | None = None,
        survey_type: str | list[str] | None = None,
        survey_type_prefix: str | None = None,
        survey_version: str | list[str] | None = None,
        survey_version_prefix: str | None = None,
        min_tie_measured_depth: int | None = None,
        max_tie_measured_depth: int | None = None,
        min_tie_true_vertical_depth: int | None = None,
        max_tie_true_vertical_depth: int | None = None,
        min_top_depth_measured_depth: int | None = None,
        max_top_depth_measured_depth: int | None = None,
        min_tortuosity: float | None = None,
        max_tortuosity: float | None = None,
        vertical_measurement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        wellbore_id: str | list[str] | None = None,
        wellbore_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellboreTrajectoryDataFields | Sequence[WellboreTrajectoryDataFields] | None = None,
        group_by: WellboreTrajectoryDataFields | Sequence[WellboreTrajectoryDataFields] = None,
        query: str | None = None,
        search_properties: WellboreTrajectoryDataTextFields | Sequence[WellboreTrajectoryDataTextFields] | None = None,
        acquisition_date: str | list[str] | None = None,
        acquisition_date_prefix: str | None = None,
        acquisition_remark: str | list[str] | None = None,
        acquisition_remark_prefix: str | None = None,
        active_indicator: bool | None = None,
        applied_operations_date_time: str | list[str] | None = None,
        applied_operations_date_time_prefix: str | None = None,
        applied_operations_remarks: str | list[str] | None = None,
        applied_operations_remarks_prefix: str | None = None,
        applied_operations_user: str | list[str] | None = None,
        applied_operations_user_prefix: str | None = None,
        azimuth_reference_type: str | list[str] | None = None,
        azimuth_reference_type_prefix: str | None = None,
        min_base_depth_measured_depth: int | None = None,
        max_base_depth_measured_depth: int | None = None,
        calculation_method_type: str | list[str] | None = None,
        calculation_method_type_prefix: str | None = None,
        company_id: str | list[str] | None = None,
        company_id_prefix: str | None = None,
        creation_date_time: str | list[str] | None = None,
        creation_date_time_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        end_date_time: str | list[str] | None = None,
        end_date_time_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        min_extrapolated_measured_depth: int | None = None,
        max_extrapolated_measured_depth: int | None = None,
        extrapolated_measured_depth_remark: str | list[str] | None = None,
        extrapolated_measured_depth_remark_prefix: str | None = None,
        geographic_crsid: str | list[str] | None = None,
        geographic_crsid_prefix: str | None = None,
        is_discoverable: bool | None = None,
        is_extended_load: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        projected_crsid: str | list[str] | None = None,
        projected_crsid_prefix: str | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        service_company_id: str | list[str] | None = None,
        service_company_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        spatial_point: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        start_date_time: str | list[str] | None = None,
        start_date_time_prefix: str | None = None,
        submitter_name: str | list[str] | None = None,
        submitter_name_prefix: str | None = None,
        min_surface_grid_convergence: float | None = None,
        max_surface_grid_convergence: float | None = None,
        min_surface_scale_factor: float | None = None,
        max_surface_scale_factor: float | None = None,
        survey_reference_identifier: str | list[str] | None = None,
        survey_reference_identifier_prefix: str | None = None,
        survey_tool_type_id: str | list[str] | None = None,
        survey_tool_type_id_prefix: str | None = None,
        survey_type: str | list[str] | None = None,
        survey_type_prefix: str | None = None,
        survey_version: str | list[str] | None = None,
        survey_version_prefix: str | None = None,
        min_tie_measured_depth: int | None = None,
        max_tie_measured_depth: int | None = None,
        min_tie_true_vertical_depth: int | None = None,
        max_tie_true_vertical_depth: int | None = None,
        min_top_depth_measured_depth: int | None = None,
        max_top_depth_measured_depth: int | None = None,
        min_tortuosity: float | None = None,
        max_tortuosity: float | None = None,
        vertical_measurement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        wellbore_id: str | list[str] | None = None,
        wellbore_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellboreTrajectoryDataFields | Sequence[WellboreTrajectoryDataFields] | None = None,
        group_by: WellboreTrajectoryDataFields | Sequence[WellboreTrajectoryDataFields] | None = None,
        query: str | None = None,
        search_property: WellboreTrajectoryDataTextFields | Sequence[WellboreTrajectoryDataTextFields] | None = None,
        acquisition_date: str | list[str] | None = None,
        acquisition_date_prefix: str | None = None,
        acquisition_remark: str | list[str] | None = None,
        acquisition_remark_prefix: str | None = None,
        active_indicator: bool | None = None,
        applied_operations_date_time: str | list[str] | None = None,
        applied_operations_date_time_prefix: str | None = None,
        applied_operations_remarks: str | list[str] | None = None,
        applied_operations_remarks_prefix: str | None = None,
        applied_operations_user: str | list[str] | None = None,
        applied_operations_user_prefix: str | None = None,
        azimuth_reference_type: str | list[str] | None = None,
        azimuth_reference_type_prefix: str | None = None,
        min_base_depth_measured_depth: int | None = None,
        max_base_depth_measured_depth: int | None = None,
        calculation_method_type: str | list[str] | None = None,
        calculation_method_type_prefix: str | None = None,
        company_id: str | list[str] | None = None,
        company_id_prefix: str | None = None,
        creation_date_time: str | list[str] | None = None,
        creation_date_time_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        end_date_time: str | list[str] | None = None,
        end_date_time_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        min_extrapolated_measured_depth: int | None = None,
        max_extrapolated_measured_depth: int | None = None,
        extrapolated_measured_depth_remark: str | list[str] | None = None,
        extrapolated_measured_depth_remark_prefix: str | None = None,
        geographic_crsid: str | list[str] | None = None,
        geographic_crsid_prefix: str | None = None,
        is_discoverable: bool | None = None,
        is_extended_load: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        projected_crsid: str | list[str] | None = None,
        projected_crsid_prefix: str | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        service_company_id: str | list[str] | None = None,
        service_company_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        spatial_point: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        start_date_time: str | list[str] | None = None,
        start_date_time_prefix: str | None = None,
        submitter_name: str | list[str] | None = None,
        submitter_name_prefix: str | None = None,
        min_surface_grid_convergence: float | None = None,
        max_surface_grid_convergence: float | None = None,
        min_surface_scale_factor: float | None = None,
        max_surface_scale_factor: float | None = None,
        survey_reference_identifier: str | list[str] | None = None,
        survey_reference_identifier_prefix: str | None = None,
        survey_tool_type_id: str | list[str] | None = None,
        survey_tool_type_id_prefix: str | None = None,
        survey_type: str | list[str] | None = None,
        survey_type_prefix: str | None = None,
        survey_version: str | list[str] | None = None,
        survey_version_prefix: str | None = None,
        min_tie_measured_depth: int | None = None,
        max_tie_measured_depth: int | None = None,
        min_tie_true_vertical_depth: int | None = None,
        max_tie_true_vertical_depth: int | None = None,
        min_top_depth_measured_depth: int | None = None,
        max_top_depth_measured_depth: int | None = None,
        min_tortuosity: float | None = None,
        max_tortuosity: float | None = None,
        vertical_measurement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        wellbore_id: str | list[str] | None = None,
        wellbore_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across wellbore trajectory data

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            acquisition_date: The acquisition date to filter on.
            acquisition_date_prefix: The prefix of the acquisition date to filter on.
            acquisition_remark: The acquisition remark to filter on.
            acquisition_remark_prefix: The prefix of the acquisition remark to filter on.
            active_indicator: The active indicator to filter on.
            applied_operations_date_time: The applied operations date time to filter on.
            applied_operations_date_time_prefix: The prefix of the applied operations date time to filter on.
            applied_operations_remarks: The applied operations remark to filter on.
            applied_operations_remarks_prefix: The prefix of the applied operations remark to filter on.
            applied_operations_user: The applied operations user to filter on.
            applied_operations_user_prefix: The prefix of the applied operations user to filter on.
            azimuth_reference_type: The azimuth reference type to filter on.
            azimuth_reference_type_prefix: The prefix of the azimuth reference type to filter on.
            min_base_depth_measured_depth: The minimum value of the base depth measured depth to filter on.
            max_base_depth_measured_depth: The maximum value of the base depth measured depth to filter on.
            calculation_method_type: The calculation method type to filter on.
            calculation_method_type_prefix: The prefix of the calculation method type to filter on.
            company_id: The company id to filter on.
            company_id_prefix: The prefix of the company id to filter on.
            creation_date_time: The creation date time to filter on.
            creation_date_time_prefix: The prefix of the creation date time to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            end_date_time: The end date time to filter on.
            end_date_time_prefix: The prefix of the end date time to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            min_extrapolated_measured_depth: The minimum value of the extrapolated measured depth to filter on.
            max_extrapolated_measured_depth: The maximum value of the extrapolated measured depth to filter on.
            extrapolated_measured_depth_remark: The extrapolated measured depth remark to filter on.
            extrapolated_measured_depth_remark_prefix: The prefix of the extrapolated measured depth remark to filter on.
            geographic_crsid: The geographic crsid to filter on.
            geographic_crsid_prefix: The prefix of the geographic crsid to filter on.
            is_discoverable: The is discoverable to filter on.
            is_extended_load: The is extended load to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            projected_crsid: The projected crsid to filter on.
            projected_crsid_prefix: The prefix of the projected crsid to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            service_company_id: The service company id to filter on.
            service_company_id_prefix: The prefix of the service company id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_area: The spatial area to filter on.
            spatial_point: The spatial point to filter on.
            start_date_time: The start date time to filter on.
            start_date_time_prefix: The prefix of the start date time to filter on.
            submitter_name: The submitter name to filter on.
            submitter_name_prefix: The prefix of the submitter name to filter on.
            min_surface_grid_convergence: The minimum value of the surface grid convergence to filter on.
            max_surface_grid_convergence: The maximum value of the surface grid convergence to filter on.
            min_surface_scale_factor: The minimum value of the surface scale factor to filter on.
            max_surface_scale_factor: The maximum value of the surface scale factor to filter on.
            survey_reference_identifier: The survey reference identifier to filter on.
            survey_reference_identifier_prefix: The prefix of the survey reference identifier to filter on.
            survey_tool_type_id: The survey tool type id to filter on.
            survey_tool_type_id_prefix: The prefix of the survey tool type id to filter on.
            survey_type: The survey type to filter on.
            survey_type_prefix: The prefix of the survey type to filter on.
            survey_version: The survey version to filter on.
            survey_version_prefix: The prefix of the survey version to filter on.
            min_tie_measured_depth: The minimum value of the tie measured depth to filter on.
            max_tie_measured_depth: The maximum value of the tie measured depth to filter on.
            min_tie_true_vertical_depth: The minimum value of the tie true vertical depth to filter on.
            max_tie_true_vertical_depth: The maximum value of the tie true vertical depth to filter on.
            min_top_depth_measured_depth: The minimum value of the top depth measured depth to filter on.
            max_top_depth_measured_depth: The maximum value of the top depth measured depth to filter on.
            min_tortuosity: The minimum value of the tortuosity to filter on.
            max_tortuosity: The maximum value of the tortuosity to filter on.
            vertical_measurement: The vertical measurement to filter on.
            wellbore_id: The wellbore id to filter on.
            wellbore_id_prefix: The prefix of the wellbore id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore trajectory data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `artefacts`, `available_trajectory_station_properties`, `geo_contexts`, `lineage_assertions`, `name_aliases` or `technical_assurances` external ids for the wellbore trajectory data. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count wellbore trajectory data in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.wellbore_trajectory_data.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            acquisition_date,
            acquisition_date_prefix,
            acquisition_remark,
            acquisition_remark_prefix,
            active_indicator,
            applied_operations_date_time,
            applied_operations_date_time_prefix,
            applied_operations_remarks,
            applied_operations_remarks_prefix,
            applied_operations_user,
            applied_operations_user_prefix,
            azimuth_reference_type,
            azimuth_reference_type_prefix,
            min_base_depth_measured_depth,
            max_base_depth_measured_depth,
            calculation_method_type,
            calculation_method_type_prefix,
            company_id,
            company_id_prefix,
            creation_date_time,
            creation_date_time_prefix,
            description,
            description_prefix,
            end_date_time,
            end_date_time_prefix,
            existence_kind,
            existence_kind_prefix,
            min_extrapolated_measured_depth,
            max_extrapolated_measured_depth,
            extrapolated_measured_depth_remark,
            extrapolated_measured_depth_remark_prefix,
            geographic_crsid,
            geographic_crsid_prefix,
            is_discoverable,
            is_extended_load,
            name,
            name_prefix,
            projected_crsid,
            projected_crsid_prefix,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            service_company_id,
            service_company_id_prefix,
            source,
            source_prefix,
            spatial_area,
            spatial_point,
            start_date_time,
            start_date_time_prefix,
            submitter_name,
            submitter_name_prefix,
            min_surface_grid_convergence,
            max_surface_grid_convergence,
            min_surface_scale_factor,
            max_surface_scale_factor,
            survey_reference_identifier,
            survey_reference_identifier_prefix,
            survey_tool_type_id,
            survey_tool_type_id_prefix,
            survey_type,
            survey_type_prefix,
            survey_version,
            survey_version_prefix,
            min_tie_measured_depth,
            max_tie_measured_depth,
            min_tie_true_vertical_depth,
            max_tie_true_vertical_depth,
            min_top_depth_measured_depth,
            max_top_depth_measured_depth,
            min_tortuosity,
            max_tortuosity,
            vertical_measurement,
            wellbore_id,
            wellbore_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WellboreTrajectoryDataFields,
        interval: float,
        query: str | None = None,
        search_property: WellboreTrajectoryDataTextFields | Sequence[WellboreTrajectoryDataTextFields] | None = None,
        acquisition_date: str | list[str] | None = None,
        acquisition_date_prefix: str | None = None,
        acquisition_remark: str | list[str] | None = None,
        acquisition_remark_prefix: str | None = None,
        active_indicator: bool | None = None,
        applied_operations_date_time: str | list[str] | None = None,
        applied_operations_date_time_prefix: str | None = None,
        applied_operations_remarks: str | list[str] | None = None,
        applied_operations_remarks_prefix: str | None = None,
        applied_operations_user: str | list[str] | None = None,
        applied_operations_user_prefix: str | None = None,
        azimuth_reference_type: str | list[str] | None = None,
        azimuth_reference_type_prefix: str | None = None,
        min_base_depth_measured_depth: int | None = None,
        max_base_depth_measured_depth: int | None = None,
        calculation_method_type: str | list[str] | None = None,
        calculation_method_type_prefix: str | None = None,
        company_id: str | list[str] | None = None,
        company_id_prefix: str | None = None,
        creation_date_time: str | list[str] | None = None,
        creation_date_time_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        end_date_time: str | list[str] | None = None,
        end_date_time_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        min_extrapolated_measured_depth: int | None = None,
        max_extrapolated_measured_depth: int | None = None,
        extrapolated_measured_depth_remark: str | list[str] | None = None,
        extrapolated_measured_depth_remark_prefix: str | None = None,
        geographic_crsid: str | list[str] | None = None,
        geographic_crsid_prefix: str | None = None,
        is_discoverable: bool | None = None,
        is_extended_load: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        projected_crsid: str | list[str] | None = None,
        projected_crsid_prefix: str | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        service_company_id: str | list[str] | None = None,
        service_company_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        spatial_point: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        start_date_time: str | list[str] | None = None,
        start_date_time_prefix: str | None = None,
        submitter_name: str | list[str] | None = None,
        submitter_name_prefix: str | None = None,
        min_surface_grid_convergence: float | None = None,
        max_surface_grid_convergence: float | None = None,
        min_surface_scale_factor: float | None = None,
        max_surface_scale_factor: float | None = None,
        survey_reference_identifier: str | list[str] | None = None,
        survey_reference_identifier_prefix: str | None = None,
        survey_tool_type_id: str | list[str] | None = None,
        survey_tool_type_id_prefix: str | None = None,
        survey_type: str | list[str] | None = None,
        survey_type_prefix: str | None = None,
        survey_version: str | list[str] | None = None,
        survey_version_prefix: str | None = None,
        min_tie_measured_depth: int | None = None,
        max_tie_measured_depth: int | None = None,
        min_tie_true_vertical_depth: int | None = None,
        max_tie_true_vertical_depth: int | None = None,
        min_top_depth_measured_depth: int | None = None,
        max_top_depth_measured_depth: int | None = None,
        min_tortuosity: float | None = None,
        max_tortuosity: float | None = None,
        vertical_measurement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        wellbore_id: str | list[str] | None = None,
        wellbore_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for wellbore trajectory data

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            acquisition_date: The acquisition date to filter on.
            acquisition_date_prefix: The prefix of the acquisition date to filter on.
            acquisition_remark: The acquisition remark to filter on.
            acquisition_remark_prefix: The prefix of the acquisition remark to filter on.
            active_indicator: The active indicator to filter on.
            applied_operations_date_time: The applied operations date time to filter on.
            applied_operations_date_time_prefix: The prefix of the applied operations date time to filter on.
            applied_operations_remarks: The applied operations remark to filter on.
            applied_operations_remarks_prefix: The prefix of the applied operations remark to filter on.
            applied_operations_user: The applied operations user to filter on.
            applied_operations_user_prefix: The prefix of the applied operations user to filter on.
            azimuth_reference_type: The azimuth reference type to filter on.
            azimuth_reference_type_prefix: The prefix of the azimuth reference type to filter on.
            min_base_depth_measured_depth: The minimum value of the base depth measured depth to filter on.
            max_base_depth_measured_depth: The maximum value of the base depth measured depth to filter on.
            calculation_method_type: The calculation method type to filter on.
            calculation_method_type_prefix: The prefix of the calculation method type to filter on.
            company_id: The company id to filter on.
            company_id_prefix: The prefix of the company id to filter on.
            creation_date_time: The creation date time to filter on.
            creation_date_time_prefix: The prefix of the creation date time to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            end_date_time: The end date time to filter on.
            end_date_time_prefix: The prefix of the end date time to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            min_extrapolated_measured_depth: The minimum value of the extrapolated measured depth to filter on.
            max_extrapolated_measured_depth: The maximum value of the extrapolated measured depth to filter on.
            extrapolated_measured_depth_remark: The extrapolated measured depth remark to filter on.
            extrapolated_measured_depth_remark_prefix: The prefix of the extrapolated measured depth remark to filter on.
            geographic_crsid: The geographic crsid to filter on.
            geographic_crsid_prefix: The prefix of the geographic crsid to filter on.
            is_discoverable: The is discoverable to filter on.
            is_extended_load: The is extended load to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            projected_crsid: The projected crsid to filter on.
            projected_crsid_prefix: The prefix of the projected crsid to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            service_company_id: The service company id to filter on.
            service_company_id_prefix: The prefix of the service company id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_area: The spatial area to filter on.
            spatial_point: The spatial point to filter on.
            start_date_time: The start date time to filter on.
            start_date_time_prefix: The prefix of the start date time to filter on.
            submitter_name: The submitter name to filter on.
            submitter_name_prefix: The prefix of the submitter name to filter on.
            min_surface_grid_convergence: The minimum value of the surface grid convergence to filter on.
            max_surface_grid_convergence: The maximum value of the surface grid convergence to filter on.
            min_surface_scale_factor: The minimum value of the surface scale factor to filter on.
            max_surface_scale_factor: The maximum value of the surface scale factor to filter on.
            survey_reference_identifier: The survey reference identifier to filter on.
            survey_reference_identifier_prefix: The prefix of the survey reference identifier to filter on.
            survey_tool_type_id: The survey tool type id to filter on.
            survey_tool_type_id_prefix: The prefix of the survey tool type id to filter on.
            survey_type: The survey type to filter on.
            survey_type_prefix: The prefix of the survey type to filter on.
            survey_version: The survey version to filter on.
            survey_version_prefix: The prefix of the survey version to filter on.
            min_tie_measured_depth: The minimum value of the tie measured depth to filter on.
            max_tie_measured_depth: The maximum value of the tie measured depth to filter on.
            min_tie_true_vertical_depth: The minimum value of the tie true vertical depth to filter on.
            max_tie_true_vertical_depth: The maximum value of the tie true vertical depth to filter on.
            min_top_depth_measured_depth: The minimum value of the top depth measured depth to filter on.
            max_top_depth_measured_depth: The maximum value of the top depth measured depth to filter on.
            min_tortuosity: The minimum value of the tortuosity to filter on.
            max_tortuosity: The maximum value of the tortuosity to filter on.
            vertical_measurement: The vertical measurement to filter on.
            wellbore_id: The wellbore id to filter on.
            wellbore_id_prefix: The prefix of the wellbore id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore trajectory data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `artefacts`, `available_trajectory_station_properties`, `geo_contexts`, `lineage_assertions`, `name_aliases` or `technical_assurances` external ids for the wellbore trajectory data. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            acquisition_date,
            acquisition_date_prefix,
            acquisition_remark,
            acquisition_remark_prefix,
            active_indicator,
            applied_operations_date_time,
            applied_operations_date_time_prefix,
            applied_operations_remarks,
            applied_operations_remarks_prefix,
            applied_operations_user,
            applied_operations_user_prefix,
            azimuth_reference_type,
            azimuth_reference_type_prefix,
            min_base_depth_measured_depth,
            max_base_depth_measured_depth,
            calculation_method_type,
            calculation_method_type_prefix,
            company_id,
            company_id_prefix,
            creation_date_time,
            creation_date_time_prefix,
            description,
            description_prefix,
            end_date_time,
            end_date_time_prefix,
            existence_kind,
            existence_kind_prefix,
            min_extrapolated_measured_depth,
            max_extrapolated_measured_depth,
            extrapolated_measured_depth_remark,
            extrapolated_measured_depth_remark_prefix,
            geographic_crsid,
            geographic_crsid_prefix,
            is_discoverable,
            is_extended_load,
            name,
            name_prefix,
            projected_crsid,
            projected_crsid_prefix,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            service_company_id,
            service_company_id_prefix,
            source,
            source_prefix,
            spatial_area,
            spatial_point,
            start_date_time,
            start_date_time_prefix,
            submitter_name,
            submitter_name_prefix,
            min_surface_grid_convergence,
            max_surface_grid_convergence,
            min_surface_scale_factor,
            max_surface_scale_factor,
            survey_reference_identifier,
            survey_reference_identifier_prefix,
            survey_tool_type_id,
            survey_tool_type_id_prefix,
            survey_type,
            survey_type_prefix,
            survey_version,
            survey_version_prefix,
            min_tie_measured_depth,
            max_tie_measured_depth,
            min_tie_true_vertical_depth,
            max_tie_true_vertical_depth,
            min_top_depth_measured_depth,
            max_top_depth_measured_depth,
            min_tortuosity,
            max_tortuosity,
            vertical_measurement,
            wellbore_id,
            wellbore_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        acquisition_date: str | list[str] | None = None,
        acquisition_date_prefix: str | None = None,
        acquisition_remark: str | list[str] | None = None,
        acquisition_remark_prefix: str | None = None,
        active_indicator: bool | None = None,
        applied_operations_date_time: str | list[str] | None = None,
        applied_operations_date_time_prefix: str | None = None,
        applied_operations_remarks: str | list[str] | None = None,
        applied_operations_remarks_prefix: str | None = None,
        applied_operations_user: str | list[str] | None = None,
        applied_operations_user_prefix: str | None = None,
        azimuth_reference_type: str | list[str] | None = None,
        azimuth_reference_type_prefix: str | None = None,
        min_base_depth_measured_depth: int | None = None,
        max_base_depth_measured_depth: int | None = None,
        calculation_method_type: str | list[str] | None = None,
        calculation_method_type_prefix: str | None = None,
        company_id: str | list[str] | None = None,
        company_id_prefix: str | None = None,
        creation_date_time: str | list[str] | None = None,
        creation_date_time_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        end_date_time: str | list[str] | None = None,
        end_date_time_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        min_extrapolated_measured_depth: int | None = None,
        max_extrapolated_measured_depth: int | None = None,
        extrapolated_measured_depth_remark: str | list[str] | None = None,
        extrapolated_measured_depth_remark_prefix: str | None = None,
        geographic_crsid: str | list[str] | None = None,
        geographic_crsid_prefix: str | None = None,
        is_discoverable: bool | None = None,
        is_extended_load: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        projected_crsid: str | list[str] | None = None,
        projected_crsid_prefix: str | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        service_company_id: str | list[str] | None = None,
        service_company_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        spatial_point: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        start_date_time: str | list[str] | None = None,
        start_date_time_prefix: str | None = None,
        submitter_name: str | list[str] | None = None,
        submitter_name_prefix: str | None = None,
        min_surface_grid_convergence: float | None = None,
        max_surface_grid_convergence: float | None = None,
        min_surface_scale_factor: float | None = None,
        max_surface_scale_factor: float | None = None,
        survey_reference_identifier: str | list[str] | None = None,
        survey_reference_identifier_prefix: str | None = None,
        survey_tool_type_id: str | list[str] | None = None,
        survey_tool_type_id_prefix: str | None = None,
        survey_type: str | list[str] | None = None,
        survey_type_prefix: str | None = None,
        survey_version: str | list[str] | None = None,
        survey_version_prefix: str | None = None,
        min_tie_measured_depth: int | None = None,
        max_tie_measured_depth: int | None = None,
        min_tie_true_vertical_depth: int | None = None,
        max_tie_true_vertical_depth: int | None = None,
        min_top_depth_measured_depth: int | None = None,
        max_top_depth_measured_depth: int | None = None,
        min_tortuosity: float | None = None,
        max_tortuosity: float | None = None,
        vertical_measurement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        wellbore_id: str | list[str] | None = None,
        wellbore_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WellboreTrajectoryDataList:
        """List/filter wellbore trajectory data

        Args:
            acquisition_date: The acquisition date to filter on.
            acquisition_date_prefix: The prefix of the acquisition date to filter on.
            acquisition_remark: The acquisition remark to filter on.
            acquisition_remark_prefix: The prefix of the acquisition remark to filter on.
            active_indicator: The active indicator to filter on.
            applied_operations_date_time: The applied operations date time to filter on.
            applied_operations_date_time_prefix: The prefix of the applied operations date time to filter on.
            applied_operations_remarks: The applied operations remark to filter on.
            applied_operations_remarks_prefix: The prefix of the applied operations remark to filter on.
            applied_operations_user: The applied operations user to filter on.
            applied_operations_user_prefix: The prefix of the applied operations user to filter on.
            azimuth_reference_type: The azimuth reference type to filter on.
            azimuth_reference_type_prefix: The prefix of the azimuth reference type to filter on.
            min_base_depth_measured_depth: The minimum value of the base depth measured depth to filter on.
            max_base_depth_measured_depth: The maximum value of the base depth measured depth to filter on.
            calculation_method_type: The calculation method type to filter on.
            calculation_method_type_prefix: The prefix of the calculation method type to filter on.
            company_id: The company id to filter on.
            company_id_prefix: The prefix of the company id to filter on.
            creation_date_time: The creation date time to filter on.
            creation_date_time_prefix: The prefix of the creation date time to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            end_date_time: The end date time to filter on.
            end_date_time_prefix: The prefix of the end date time to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            min_extrapolated_measured_depth: The minimum value of the extrapolated measured depth to filter on.
            max_extrapolated_measured_depth: The maximum value of the extrapolated measured depth to filter on.
            extrapolated_measured_depth_remark: The extrapolated measured depth remark to filter on.
            extrapolated_measured_depth_remark_prefix: The prefix of the extrapolated measured depth remark to filter on.
            geographic_crsid: The geographic crsid to filter on.
            geographic_crsid_prefix: The prefix of the geographic crsid to filter on.
            is_discoverable: The is discoverable to filter on.
            is_extended_load: The is extended load to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            projected_crsid: The projected crsid to filter on.
            projected_crsid_prefix: The prefix of the projected crsid to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            service_company_id: The service company id to filter on.
            service_company_id_prefix: The prefix of the service company id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_area: The spatial area to filter on.
            spatial_point: The spatial point to filter on.
            start_date_time: The start date time to filter on.
            start_date_time_prefix: The prefix of the start date time to filter on.
            submitter_name: The submitter name to filter on.
            submitter_name_prefix: The prefix of the submitter name to filter on.
            min_surface_grid_convergence: The minimum value of the surface grid convergence to filter on.
            max_surface_grid_convergence: The maximum value of the surface grid convergence to filter on.
            min_surface_scale_factor: The minimum value of the surface scale factor to filter on.
            max_surface_scale_factor: The maximum value of the surface scale factor to filter on.
            survey_reference_identifier: The survey reference identifier to filter on.
            survey_reference_identifier_prefix: The prefix of the survey reference identifier to filter on.
            survey_tool_type_id: The survey tool type id to filter on.
            survey_tool_type_id_prefix: The prefix of the survey tool type id to filter on.
            survey_type: The survey type to filter on.
            survey_type_prefix: The prefix of the survey type to filter on.
            survey_version: The survey version to filter on.
            survey_version_prefix: The prefix of the survey version to filter on.
            min_tie_measured_depth: The minimum value of the tie measured depth to filter on.
            max_tie_measured_depth: The maximum value of the tie measured depth to filter on.
            min_tie_true_vertical_depth: The minimum value of the tie true vertical depth to filter on.
            max_tie_true_vertical_depth: The maximum value of the tie true vertical depth to filter on.
            min_top_depth_measured_depth: The minimum value of the top depth measured depth to filter on.
            max_top_depth_measured_depth: The maximum value of the top depth measured depth to filter on.
            min_tortuosity: The minimum value of the tortuosity to filter on.
            max_tortuosity: The maximum value of the tortuosity to filter on.
            vertical_measurement: The vertical measurement to filter on.
            wellbore_id: The wellbore id to filter on.
            wellbore_id_prefix: The prefix of the wellbore id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore trajectory data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `artefacts`, `available_trajectory_station_properties`, `geo_contexts`, `lineage_assertions`, `name_aliases` or `technical_assurances` external ids for the wellbore trajectory data. Defaults to True.

        Returns:
            List of requested wellbore trajectory data

        Examples:

            List wellbore trajectory data and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_data = client.wellbore_trajectory_data.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            acquisition_date,
            acquisition_date_prefix,
            acquisition_remark,
            acquisition_remark_prefix,
            active_indicator,
            applied_operations_date_time,
            applied_operations_date_time_prefix,
            applied_operations_remarks,
            applied_operations_remarks_prefix,
            applied_operations_user,
            applied_operations_user_prefix,
            azimuth_reference_type,
            azimuth_reference_type_prefix,
            min_base_depth_measured_depth,
            max_base_depth_measured_depth,
            calculation_method_type,
            calculation_method_type_prefix,
            company_id,
            company_id_prefix,
            creation_date_time,
            creation_date_time_prefix,
            description,
            description_prefix,
            end_date_time,
            end_date_time_prefix,
            existence_kind,
            existence_kind_prefix,
            min_extrapolated_measured_depth,
            max_extrapolated_measured_depth,
            extrapolated_measured_depth_remark,
            extrapolated_measured_depth_remark_prefix,
            geographic_crsid,
            geographic_crsid_prefix,
            is_discoverable,
            is_extended_load,
            name,
            name_prefix,
            projected_crsid,
            projected_crsid_prefix,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            service_company_id,
            service_company_id_prefix,
            source,
            source_prefix,
            spatial_area,
            spatial_point,
            start_date_time,
            start_date_time_prefix,
            submitter_name,
            submitter_name_prefix,
            min_surface_grid_convergence,
            max_surface_grid_convergence,
            min_surface_scale_factor,
            max_surface_scale_factor,
            survey_reference_identifier,
            survey_reference_identifier_prefix,
            survey_tool_type_id,
            survey_tool_type_id_prefix,
            survey_type,
            survey_type_prefix,
            survey_version,
            survey_version_prefix,
            min_tie_measured_depth,
            max_tie_measured_depth,
            min_tie_true_vertical_depth,
            max_tie_true_vertical_depth,
            min_top_depth_measured_depth,
            max_top_depth_measured_depth,
            min_tortuosity,
            max_tortuosity,
            vertical_measurement,
            wellbore_id,
            wellbore_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        wellbore_trajectory_data = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            space_arg = {"space": space} if space else {}
            if len(ids := wellbore_trajectory_data.as_node_ids()) > IN_FILTER_LIMIT:
                artefact_edges = self.artefacts.list(limit=-1, **space_arg)
            else:
                artefact_edges = self.artefacts.list(ids, limit=-1)
            self._set_artefacts(wellbore_trajectory_data, artefact_edges)
            if len(ids := wellbore_trajectory_data.as_node_ids()) > IN_FILTER_LIMIT:
                available_trajectory_station_property_edges = self.available_trajectory_station_properties.list(
                    limit=-1, **space_arg
                )
            else:
                available_trajectory_station_property_edges = self.available_trajectory_station_properties.list(
                    ids, limit=-1
                )
            self._set_available_trajectory_station_properties(
                wellbore_trajectory_data, available_trajectory_station_property_edges
            )
            if len(ids := wellbore_trajectory_data.as_node_ids()) > IN_FILTER_LIMIT:
                geo_context_edges = self.geo_contexts.list(limit=-1, **space_arg)
            else:
                geo_context_edges = self.geo_contexts.list(ids, limit=-1)
            self._set_geo_contexts(wellbore_trajectory_data, geo_context_edges)
            if len(ids := wellbore_trajectory_data.as_node_ids()) > IN_FILTER_LIMIT:
                lineage_assertion_edges = self.lineage_assertions.list(limit=-1, **space_arg)
            else:
                lineage_assertion_edges = self.lineage_assertions.list(ids, limit=-1)
            self._set_lineage_assertions(wellbore_trajectory_data, lineage_assertion_edges)
            if len(ids := wellbore_trajectory_data.as_node_ids()) > IN_FILTER_LIMIT:
                name_alias_edges = self.name_aliases.list(limit=-1, **space_arg)
            else:
                name_alias_edges = self.name_aliases.list(ids, limit=-1)
            self._set_name_aliases(wellbore_trajectory_data, name_alias_edges)
            if len(ids := wellbore_trajectory_data.as_node_ids()) > IN_FILTER_LIMIT:
                technical_assurance_edges = self.technical_assurances.list(limit=-1, **space_arg)
            else:
                technical_assurance_edges = self.technical_assurances.list(ids, limit=-1)
            self._set_technical_assurances(wellbore_trajectory_data, technical_assurance_edges)

        return wellbore_trajectory_data

    @staticmethod
    def _set_artefacts(wellbore_trajectory_data: Sequence[WellboreTrajectoryData], artefact_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in artefact_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_trajectory_datum in wellbore_trajectory_data:
            node_id = wellbore_trajectory_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_trajectory_datum.artefacts = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_available_trajectory_station_properties(
        wellbore_trajectory_data: Sequence[WellboreTrajectoryData],
        available_trajectory_station_property_edges: Sequence[dm.Edge],
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in available_trajectory_station_property_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_trajectory_datum in wellbore_trajectory_data:
            node_id = wellbore_trajectory_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_trajectory_datum.available_trajectory_station_properties = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_geo_contexts(
        wellbore_trajectory_data: Sequence[WellboreTrajectoryData], geo_context_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in geo_context_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_trajectory_datum in wellbore_trajectory_data:
            node_id = wellbore_trajectory_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_trajectory_datum.geo_contexts = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_lineage_assertions(
        wellbore_trajectory_data: Sequence[WellboreTrajectoryData], lineage_assertion_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in lineage_assertion_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_trajectory_datum in wellbore_trajectory_data:
            node_id = wellbore_trajectory_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_trajectory_datum.lineage_assertions = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_name_aliases(
        wellbore_trajectory_data: Sequence[WellboreTrajectoryData], name_alias_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in name_alias_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_trajectory_datum in wellbore_trajectory_data:
            node_id = wellbore_trajectory_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_trajectory_datum.name_aliases = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_technical_assurances(
        wellbore_trajectory_data: Sequence[WellboreTrajectoryData], technical_assurance_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in technical_assurance_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_trajectory_datum in wellbore_trajectory_data:
            node_id = wellbore_trajectory_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_trajectory_datum.technical_assurances = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]


def _create_filter(
    view_id: dm.ViewId,
    acquisition_date: str | list[str] | None = None,
    acquisition_date_prefix: str | None = None,
    acquisition_remark: str | list[str] | None = None,
    acquisition_remark_prefix: str | None = None,
    active_indicator: bool | None = None,
    applied_operations_date_time: str | list[str] | None = None,
    applied_operations_date_time_prefix: str | None = None,
    applied_operations_remarks: str | list[str] | None = None,
    applied_operations_remarks_prefix: str | None = None,
    applied_operations_user: str | list[str] | None = None,
    applied_operations_user_prefix: str | None = None,
    azimuth_reference_type: str | list[str] | None = None,
    azimuth_reference_type_prefix: str | None = None,
    min_base_depth_measured_depth: int | None = None,
    max_base_depth_measured_depth: int | None = None,
    calculation_method_type: str | list[str] | None = None,
    calculation_method_type_prefix: str | None = None,
    company_id: str | list[str] | None = None,
    company_id_prefix: str | None = None,
    creation_date_time: str | list[str] | None = None,
    creation_date_time_prefix: str | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    end_date_time: str | list[str] | None = None,
    end_date_time_prefix: str | None = None,
    existence_kind: str | list[str] | None = None,
    existence_kind_prefix: str | None = None,
    min_extrapolated_measured_depth: int | None = None,
    max_extrapolated_measured_depth: int | None = None,
    extrapolated_measured_depth_remark: str | list[str] | None = None,
    extrapolated_measured_depth_remark_prefix: str | None = None,
    geographic_crsid: str | list[str] | None = None,
    geographic_crsid_prefix: str | None = None,
    is_discoverable: bool | None = None,
    is_extended_load: bool | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    projected_crsid: str | list[str] | None = None,
    projected_crsid_prefix: str | None = None,
    resource_curation_status: str | list[str] | None = None,
    resource_curation_status_prefix: str | None = None,
    resource_home_region_id: str | list[str] | None = None,
    resource_home_region_id_prefix: str | None = None,
    resource_lifecycle_status: str | list[str] | None = None,
    resource_lifecycle_status_prefix: str | None = None,
    resource_security_classification: str | list[str] | None = None,
    resource_security_classification_prefix: str | None = None,
    service_company_id: str | list[str] | None = None,
    service_company_id_prefix: str | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
    spatial_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    spatial_point: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    start_date_time: str | list[str] | None = None,
    start_date_time_prefix: str | None = None,
    submitter_name: str | list[str] | None = None,
    submitter_name_prefix: str | None = None,
    min_surface_grid_convergence: float | None = None,
    max_surface_grid_convergence: float | None = None,
    min_surface_scale_factor: float | None = None,
    max_surface_scale_factor: float | None = None,
    survey_reference_identifier: str | list[str] | None = None,
    survey_reference_identifier_prefix: str | None = None,
    survey_tool_type_id: str | list[str] | None = None,
    survey_tool_type_id_prefix: str | None = None,
    survey_type: str | list[str] | None = None,
    survey_type_prefix: str | None = None,
    survey_version: str | list[str] | None = None,
    survey_version_prefix: str | None = None,
    min_tie_measured_depth: int | None = None,
    max_tie_measured_depth: int | None = None,
    min_tie_true_vertical_depth: int | None = None,
    max_tie_true_vertical_depth: int | None = None,
    min_top_depth_measured_depth: int | None = None,
    max_top_depth_measured_depth: int | None = None,
    min_tortuosity: float | None = None,
    max_tortuosity: float | None = None,
    vertical_measurement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    wellbore_id: str | list[str] | None = None,
    wellbore_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if acquisition_date and isinstance(acquisition_date, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AcquisitionDate"), value=acquisition_date))
    if acquisition_date and isinstance(acquisition_date, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AcquisitionDate"), values=acquisition_date))
    if acquisition_date_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("AcquisitionDate"), value=acquisition_date_prefix))
    if acquisition_remark and isinstance(acquisition_remark, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AcquisitionRemark"), value=acquisition_remark))
    if acquisition_remark and isinstance(acquisition_remark, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AcquisitionRemark"), values=acquisition_remark))
    if acquisition_remark_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("AcquisitionRemark"), value=acquisition_remark_prefix))
    if active_indicator and isinstance(active_indicator, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ActiveIndicator"), value=active_indicator))
    if applied_operations_date_time and isinstance(applied_operations_date_time, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("AppliedOperationsDateTime"), value=applied_operations_date_time)
        )
    if applied_operations_date_time and isinstance(applied_operations_date_time, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("AppliedOperationsDateTime"), values=applied_operations_date_time)
        )
    if applied_operations_date_time_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("AppliedOperationsDateTime"), value=applied_operations_date_time_prefix
            )
        )
    if applied_operations_remarks and isinstance(applied_operations_remarks, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("AppliedOperationsRemarks"), value=applied_operations_remarks)
        )
    if applied_operations_remarks and isinstance(applied_operations_remarks, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("AppliedOperationsRemarks"), values=applied_operations_remarks)
        )
    if applied_operations_remarks_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("AppliedOperationsRemarks"), value=applied_operations_remarks_prefix
            )
        )
    if applied_operations_user and isinstance(applied_operations_user, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("AppliedOperationsUser"), value=applied_operations_user)
        )
    if applied_operations_user and isinstance(applied_operations_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AppliedOperationsUser"), values=applied_operations_user))
    if applied_operations_user_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("AppliedOperationsUser"), value=applied_operations_user_prefix)
        )
    if azimuth_reference_type and isinstance(azimuth_reference_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AzimuthReferenceType"), value=azimuth_reference_type))
    if azimuth_reference_type and isinstance(azimuth_reference_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AzimuthReferenceType"), values=azimuth_reference_type))
    if azimuth_reference_type_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("AzimuthReferenceType"), value=azimuth_reference_type_prefix)
        )
    if min_base_depth_measured_depth or max_base_depth_measured_depth:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("BaseDepthMeasuredDepth"),
                gte=min_base_depth_measured_depth,
                lte=max_base_depth_measured_depth,
            )
        )
    if calculation_method_type and isinstance(calculation_method_type, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("CalculationMethodType"), value=calculation_method_type)
        )
    if calculation_method_type and isinstance(calculation_method_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("CalculationMethodType"), values=calculation_method_type))
    if calculation_method_type_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("CalculationMethodType"), value=calculation_method_type_prefix)
        )
    if company_id and isinstance(company_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("CompanyID"), value=company_id))
    if company_id and isinstance(company_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("CompanyID"), values=company_id))
    if company_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("CompanyID"), value=company_id_prefix))
    if creation_date_time and isinstance(creation_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("CreationDateTime"), value=creation_date_time))
    if creation_date_time and isinstance(creation_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("CreationDateTime"), values=creation_date_time))
    if creation_date_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("CreationDateTime"), value=creation_date_time_prefix))
    if description and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Description"), value=description_prefix))
    if end_date_time and isinstance(end_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EndDateTime"), value=end_date_time))
    if end_date_time and isinstance(end_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EndDateTime"), values=end_date_time))
    if end_date_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("EndDateTime"), value=end_date_time_prefix))
    if existence_kind and isinstance(existence_kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ExistenceKind"), value=existence_kind))
    if existence_kind and isinstance(existence_kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ExistenceKind"), values=existence_kind))
    if existence_kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ExistenceKind"), value=existence_kind_prefix))
    if min_extrapolated_measured_depth or max_extrapolated_measured_depth:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("ExtrapolatedMeasuredDepth"),
                gte=min_extrapolated_measured_depth,
                lte=max_extrapolated_measured_depth,
            )
        )
    if extrapolated_measured_depth_remark and isinstance(extrapolated_measured_depth_remark, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("ExtrapolatedMeasuredDepthRemark"), value=extrapolated_measured_depth_remark
            )
        )
    if extrapolated_measured_depth_remark and isinstance(extrapolated_measured_depth_remark, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("ExtrapolatedMeasuredDepthRemark"), values=extrapolated_measured_depth_remark
            )
        )
    if extrapolated_measured_depth_remark_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("ExtrapolatedMeasuredDepthRemark"),
                value=extrapolated_measured_depth_remark_prefix,
            )
        )
    if geographic_crsid and isinstance(geographic_crsid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("GeographicCRSID"), value=geographic_crsid))
    if geographic_crsid and isinstance(geographic_crsid, list):
        filters.append(dm.filters.In(view_id.as_property_ref("GeographicCRSID"), values=geographic_crsid))
    if geographic_crsid_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("GeographicCRSID"), value=geographic_crsid_prefix))
    if is_discoverable and isinstance(is_discoverable, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("IsDiscoverable"), value=is_discoverable))
    if is_extended_load and isinstance(is_extended_load, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("IsExtendedLoad"), value=is_extended_load))
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Name"), value=name_prefix))
    if projected_crsid and isinstance(projected_crsid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ProjectedCRSID"), value=projected_crsid))
    if projected_crsid and isinstance(projected_crsid, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ProjectedCRSID"), values=projected_crsid))
    if projected_crsid_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ProjectedCRSID"), value=projected_crsid_prefix))
    if resource_curation_status and isinstance(resource_curation_status, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("ResourceCurationStatus"), value=resource_curation_status)
        )
    if resource_curation_status and isinstance(resource_curation_status, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("ResourceCurationStatus"), values=resource_curation_status)
        )
    if resource_curation_status_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("ResourceCurationStatus"), value=resource_curation_status_prefix)
        )
    if resource_home_region_id and isinstance(resource_home_region_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("ResourceHomeRegionID"), value=resource_home_region_id)
        )
    if resource_home_region_id and isinstance(resource_home_region_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ResourceHomeRegionID"), values=resource_home_region_id))
    if resource_home_region_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("ResourceHomeRegionID"), value=resource_home_region_id_prefix)
        )
    if resource_lifecycle_status and isinstance(resource_lifecycle_status, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("ResourceLifecycleStatus"), value=resource_lifecycle_status)
        )
    if resource_lifecycle_status and isinstance(resource_lifecycle_status, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("ResourceLifecycleStatus"), values=resource_lifecycle_status)
        )
    if resource_lifecycle_status_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("ResourceLifecycleStatus"), value=resource_lifecycle_status_prefix
            )
        )
    if resource_security_classification and isinstance(resource_security_classification, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("ResourceSecurityClassification"), value=resource_security_classification
            )
        )
    if resource_security_classification and isinstance(resource_security_classification, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("ResourceSecurityClassification"), values=resource_security_classification
            )
        )
    if resource_security_classification_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("ResourceSecurityClassification"), value=resource_security_classification_prefix
            )
        )
    if service_company_id and isinstance(service_company_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ServiceCompanyID"), value=service_company_id))
    if service_company_id and isinstance(service_company_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ServiceCompanyID"), values=service_company_id))
    if service_company_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ServiceCompanyID"), value=service_company_id_prefix))
    if source and isinstance(source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Source"), value=source))
    if source and isinstance(source, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Source"), values=source))
    if source_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Source"), value=source_prefix))
    if spatial_area and isinstance(spatial_area, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialArea"),
                value={"space": "IntegrationTestsImmutable", "externalId": spatial_area},
            )
        )
    if spatial_area and isinstance(spatial_area, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialArea"), value={"space": spatial_area[0], "externalId": spatial_area[1]}
            )
        )
    if spatial_area and isinstance(spatial_area, list) and isinstance(spatial_area[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialArea"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in spatial_area],
            )
        )
    if spatial_area and isinstance(spatial_area, list) and isinstance(spatial_area[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialArea"),
                values=[{"space": item[0], "externalId": item[1]} for item in spatial_area],
            )
        )
    if spatial_point and isinstance(spatial_point, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialPoint"),
                value={"space": "IntegrationTestsImmutable", "externalId": spatial_point},
            )
        )
    if spatial_point and isinstance(spatial_point, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialPoint"),
                value={"space": spatial_point[0], "externalId": spatial_point[1]},
            )
        )
    if spatial_point and isinstance(spatial_point, list) and isinstance(spatial_point[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialPoint"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in spatial_point],
            )
        )
    if spatial_point and isinstance(spatial_point, list) and isinstance(spatial_point[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialPoint"),
                values=[{"space": item[0], "externalId": item[1]} for item in spatial_point],
            )
        )
    if start_date_time and isinstance(start_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("StartDateTime"), value=start_date_time))
    if start_date_time and isinstance(start_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("StartDateTime"), values=start_date_time))
    if start_date_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("StartDateTime"), value=start_date_time_prefix))
    if submitter_name and isinstance(submitter_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("SubmitterName"), value=submitter_name))
    if submitter_name and isinstance(submitter_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("SubmitterName"), values=submitter_name))
    if submitter_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("SubmitterName"), value=submitter_name_prefix))
    if min_surface_grid_convergence or max_surface_grid_convergence:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("SurfaceGridConvergence"),
                gte=min_surface_grid_convergence,
                lte=max_surface_grid_convergence,
            )
        )
    if min_surface_scale_factor or max_surface_scale_factor:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("SurfaceScaleFactor"),
                gte=min_surface_scale_factor,
                lte=max_surface_scale_factor,
            )
        )
    if survey_reference_identifier and isinstance(survey_reference_identifier, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("SurveyReferenceIdentifier"), value=survey_reference_identifier)
        )
    if survey_reference_identifier and isinstance(survey_reference_identifier, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("SurveyReferenceIdentifier"), values=survey_reference_identifier)
        )
    if survey_reference_identifier_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("SurveyReferenceIdentifier"), value=survey_reference_identifier_prefix
            )
        )
    if survey_tool_type_id and isinstance(survey_tool_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("SurveyToolTypeID"), value=survey_tool_type_id))
    if survey_tool_type_id and isinstance(survey_tool_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("SurveyToolTypeID"), values=survey_tool_type_id))
    if survey_tool_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("SurveyToolTypeID"), value=survey_tool_type_id_prefix))
    if survey_type and isinstance(survey_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("SurveyType"), value=survey_type))
    if survey_type and isinstance(survey_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("SurveyType"), values=survey_type))
    if survey_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("SurveyType"), value=survey_type_prefix))
    if survey_version and isinstance(survey_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("SurveyVersion"), value=survey_version))
    if survey_version and isinstance(survey_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("SurveyVersion"), values=survey_version))
    if survey_version_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("SurveyVersion"), value=survey_version_prefix))
    if min_tie_measured_depth or max_tie_measured_depth:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("TieMeasuredDepth"), gte=min_tie_measured_depth, lte=max_tie_measured_depth
            )
        )
    if min_tie_true_vertical_depth or max_tie_true_vertical_depth:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("TieTrueVerticalDepth"),
                gte=min_tie_true_vertical_depth,
                lte=max_tie_true_vertical_depth,
            )
        )
    if min_top_depth_measured_depth or max_top_depth_measured_depth:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("TopDepthMeasuredDepth"),
                gte=min_top_depth_measured_depth,
                lte=max_top_depth_measured_depth,
            )
        )
    if min_tortuosity or max_tortuosity:
        filters.append(dm.filters.Range(view_id.as_property_ref("Tortuosity"), gte=min_tortuosity, lte=max_tortuosity))
    if vertical_measurement and isinstance(vertical_measurement, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalMeasurement"),
                value={"space": "IntegrationTestsImmutable", "externalId": vertical_measurement},
            )
        )
    if vertical_measurement and isinstance(vertical_measurement, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalMeasurement"),
                value={"space": vertical_measurement[0], "externalId": vertical_measurement[1]},
            )
        )
    if vertical_measurement and isinstance(vertical_measurement, list) and isinstance(vertical_measurement[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("VerticalMeasurement"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in vertical_measurement],
            )
        )
    if vertical_measurement and isinstance(vertical_measurement, list) and isinstance(vertical_measurement[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("VerticalMeasurement"),
                values=[{"space": item[0], "externalId": item[1]} for item in vertical_measurement],
            )
        )
    if wellbore_id and isinstance(wellbore_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("WellboreID"), value=wellbore_id))
    if wellbore_id and isinstance(wellbore_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("WellboreID"), values=wellbore_id))
    if wellbore_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("WellboreID"), value=wellbore_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
