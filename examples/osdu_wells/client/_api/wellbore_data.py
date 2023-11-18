from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    WellboreData,
    WellboreDataApply,
    WellboreDataList,
    WellboreDataApplyList,
    WellboreDataFields,
    WellboreDataTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._wellbore_data import _WELLBOREDATA_PROPERTIES_BY_FIELD


class WellboreDataDrillingReasonsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more drilling_reasons edges by id(s) of a wellbore datum.

        Args:
            external_id: External id or list of external ids source wellbore datum.
            space: The space where all the drilling reason edges are located.

        Returns:
            The requested drilling reason edges.

        Examples:

            Retrieve drilling_reasons edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.drilling_reasons.retrieve("my_drilling_reasons")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.DrillingReasons"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_data))

    def list(
        self,
        wellbore_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List drilling_reasons edges of a wellbore datum.

        Args:
            wellbore_datum_id: ID of the source wellbore datum.
            limit: Maximum number of drilling reason edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the drilling reason edges are located.

        Returns:
            The requested drilling reason edges.

        Examples:

            List 5 drilling_reasons edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.drilling_reasons.list("my_wellbore_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.DrillingReasons"},
            )
        ]
        if wellbore_datum_id:
            wellbore_datum_ids = wellbore_datum_id if isinstance(wellbore_datum_id, list) else [wellbore_datum_id]
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_datum_ids
                ],
            )
            filters.append(is_wellbore_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreDataFacilityEventsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more facility_events edges by id(s) of a wellbore datum.

        Args:
            external_id: External id or list of external ids source wellbore datum.
            space: The space where all the facility event edges are located.

        Returns:
            The requested facility event edges.

        Examples:

            Retrieve facility_events edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.facility_events.retrieve("my_facility_events")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.FacilityEvents"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_data))

    def list(
        self,
        wellbore_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List facility_events edges of a wellbore datum.

        Args:
            wellbore_datum_id: ID of the source wellbore datum.
            limit: Maximum number of facility event edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the facility event edges are located.

        Returns:
            The requested facility event edges.

        Examples:

            List 5 facility_events edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.facility_events.list("my_wellbore_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.FacilityEvents"},
            )
        ]
        if wellbore_datum_id:
            wellbore_datum_ids = wellbore_datum_id if isinstance(wellbore_datum_id, list) else [wellbore_datum_id]
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_datum_ids
                ],
            )
            filters.append(is_wellbore_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreDataFacilityOperatorsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more facility_operators edges by id(s) of a wellbore datum.

        Args:
            external_id: External id or list of external ids source wellbore datum.
            space: The space where all the facility operator edges are located.

        Returns:
            The requested facility operator edges.

        Examples:

            Retrieve facility_operators edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.facility_operators.retrieve("my_facility_operators")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.FacilityOperators"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_data))

    def list(
        self,
        wellbore_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List facility_operators edges of a wellbore datum.

        Args:
            wellbore_datum_id: ID of the source wellbore datum.
            limit: Maximum number of facility operator edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the facility operator edges are located.

        Returns:
            The requested facility operator edges.

        Examples:

            List 5 facility_operators edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.facility_operators.list("my_wellbore_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.FacilityOperators"},
            )
        ]
        if wellbore_datum_id:
            wellbore_datum_ids = wellbore_datum_id if isinstance(wellbore_datum_id, list) else [wellbore_datum_id]
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_datum_ids
                ],
            )
            filters.append(is_wellbore_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreDataFacilitySpecificationsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more facility_specifications edges by id(s) of a wellbore datum.

        Args:
            external_id: External id or list of external ids source wellbore datum.
            space: The space where all the facility specification edges are located.

        Returns:
            The requested facility specification edges.

        Examples:

            Retrieve facility_specifications edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.facility_specifications.retrieve("my_facility_specifications")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.FacilitySpecifications"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_data))

    def list(
        self,
        wellbore_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List facility_specifications edges of a wellbore datum.

        Args:
            wellbore_datum_id: ID of the source wellbore datum.
            limit: Maximum number of facility specification edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the facility specification edges are located.

        Returns:
            The requested facility specification edges.

        Examples:

            List 5 facility_specifications edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.facility_specifications.list("my_wellbore_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.FacilitySpecifications"},
            )
        ]
        if wellbore_datum_id:
            wellbore_datum_ids = wellbore_datum_id if isinstance(wellbore_datum_id, list) else [wellbore_datum_id]
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_datum_ids
                ],
            )
            filters.append(is_wellbore_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreDataFacilityStatesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more facility_states edges by id(s) of a wellbore datum.

        Args:
            external_id: External id or list of external ids source wellbore datum.
            space: The space where all the facility state edges are located.

        Returns:
            The requested facility state edges.

        Examples:

            Retrieve facility_states edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.facility_states.retrieve("my_facility_states")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.FacilityStates"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_data))

    def list(
        self,
        wellbore_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List facility_states edges of a wellbore datum.

        Args:
            wellbore_datum_id: ID of the source wellbore datum.
            limit: Maximum number of facility state edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the facility state edges are located.

        Returns:
            The requested facility state edges.

        Examples:

            List 5 facility_states edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.facility_states.list("my_wellbore_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.FacilityStates"},
            )
        ]
        if wellbore_datum_id:
            wellbore_datum_ids = wellbore_datum_id if isinstance(wellbore_datum_id, list) else [wellbore_datum_id]
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_datum_ids
                ],
            )
            filters.append(is_wellbore_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreDataGeoContextsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more geo_contexts edges by id(s) of a wellbore datum.

        Args:
            external_id: External id or list of external ids source wellbore datum.
            space: The space where all the geo context edges are located.

        Returns:
            The requested geo context edges.

        Examples:

            Retrieve geo_contexts edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.geo_contexts.retrieve("my_geo_contexts")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.GeoContexts"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_data))

    def list(
        self,
        wellbore_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List geo_contexts edges of a wellbore datum.

        Args:
            wellbore_datum_id: ID of the source wellbore datum.
            limit: Maximum number of geo context edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the geo context edges are located.

        Returns:
            The requested geo context edges.

        Examples:

            List 5 geo_contexts edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.geo_contexts.list("my_wellbore_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.GeoContexts"},
            )
        ]
        if wellbore_datum_id:
            wellbore_datum_ids = wellbore_datum_id if isinstance(wellbore_datum_id, list) else [wellbore_datum_id]
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_datum_ids
                ],
            )
            filters.append(is_wellbore_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreDataHistoricalInterestsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more historical_interests edges by id(s) of a wellbore datum.

        Args:
            external_id: External id or list of external ids source wellbore datum.
            space: The space where all the historical interest edges are located.

        Returns:
            The requested historical interest edges.

        Examples:

            Retrieve historical_interests edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.historical_interests.retrieve("my_historical_interests")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.HistoricalInterests"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_data))

    def list(
        self,
        wellbore_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List historical_interests edges of a wellbore datum.

        Args:
            wellbore_datum_id: ID of the source wellbore datum.
            limit: Maximum number of historical interest edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the historical interest edges are located.

        Returns:
            The requested historical interest edges.

        Examples:

            List 5 historical_interests edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.historical_interests.list("my_wellbore_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.HistoricalInterests"},
            )
        ]
        if wellbore_datum_id:
            wellbore_datum_ids = wellbore_datum_id if isinstance(wellbore_datum_id, list) else [wellbore_datum_id]
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_datum_ids
                ],
            )
            filters.append(is_wellbore_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreDataNameAliasesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more name_aliases edges by id(s) of a wellbore datum.

        Args:
            external_id: External id or list of external ids source wellbore datum.
            space: The space where all the name alias edges are located.

        Returns:
            The requested name alias edges.

        Examples:

            Retrieve name_aliases edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.name_aliases.retrieve("my_name_aliases")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.NameAliases"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_data))

    def list(
        self,
        wellbore_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List name_aliases edges of a wellbore datum.

        Args:
            wellbore_datum_id: ID of the source wellbore datum.
            limit: Maximum number of name alias edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the name alias edges are located.

        Returns:
            The requested name alias edges.

        Examples:

            List 5 name_aliases edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.name_aliases.list("my_wellbore_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.NameAliases"},
            )
        ]
        if wellbore_datum_id:
            wellbore_datum_ids = wellbore_datum_id if isinstance(wellbore_datum_id, list) else [wellbore_datum_id]
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_datum_ids
                ],
            )
            filters.append(is_wellbore_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreDataTechnicalAssurancesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more technical_assurances edges by id(s) of a wellbore datum.

        Args:
            external_id: External id or list of external ids source wellbore datum.
            space: The space where all the technical assurance edges are located.

        Returns:
            The requested technical assurance edges.

        Examples:

            Retrieve technical_assurances edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.technical_assurances.retrieve("my_technical_assurances")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.TechnicalAssurances"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_data))

    def list(
        self,
        wellbore_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List technical_assurances edges of a wellbore datum.

        Args:
            wellbore_datum_id: ID of the source wellbore datum.
            limit: Maximum number of technical assurance edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the technical assurance edges are located.

        Returns:
            The requested technical assurance edges.

        Examples:

            List 5 technical_assurances edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.technical_assurances.list("my_wellbore_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.TechnicalAssurances"},
            )
        ]
        if wellbore_datum_id:
            wellbore_datum_ids = wellbore_datum_id if isinstance(wellbore_datum_id, list) else [wellbore_datum_id]
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_datum_ids
                ],
            )
            filters.append(is_wellbore_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreDataVerticalMeasurementsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more vertical_measurements edges by id(s) of a wellbore datum.

        Args:
            external_id: External id or list of external ids source wellbore datum.
            space: The space where all the vertical measurement edges are located.

        Returns:
            The requested vertical measurement edges.

        Examples:

            Retrieve vertical_measurements edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.vertical_measurements.retrieve("my_vertical_measurements")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.VerticalMeasurements"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_data))

    def list(
        self,
        wellbore_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List vertical_measurements edges of a wellbore datum.

        Args:
            wellbore_datum_id: ID of the source wellbore datum.
            limit: Maximum number of vertical measurement edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the vertical measurement edges are located.

        Returns:
            The requested vertical measurement edges.

        Examples:

            List 5 vertical_measurements edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.vertical_measurements.list("my_wellbore_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.VerticalMeasurements"},
            )
        ]
        if wellbore_datum_id:
            wellbore_datum_ids = wellbore_datum_id if isinstance(wellbore_datum_id, list) else [wellbore_datum_id]
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_datum_ids
                ],
            )
            filters.append(is_wellbore_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreDataWellboreCostsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more wellbore_costs edges by id(s) of a wellbore datum.

        Args:
            external_id: External id or list of external ids source wellbore datum.
            space: The space where all the wellbore cost edges are located.

        Returns:
            The requested wellbore cost edges.

        Examples:

            Retrieve wellbore_costs edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.wellbore_costs.retrieve("my_wellbore_costs")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.WellboreCosts"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_wellbore_data = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_wellbore_data))

    def list(
        self,
        wellbore_datum_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List wellbore_costs edges of a wellbore datum.

        Args:
            wellbore_datum_id: ID of the source wellbore datum.
            limit: Maximum number of wellbore cost edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the wellbore cost edges are located.

        Returns:
            The requested wellbore cost edges.

        Examples:

            List 5 wellbore_costs edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.wellbore_costs.list("my_wellbore_datum", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "WellboreData.WellboreCosts"},
            )
        ]
        if wellbore_datum_id:
            wellbore_datum_ids = wellbore_datum_id if isinstance(wellbore_datum_id, list) else [wellbore_datum_id]
            is_wellbore_data = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in wellbore_datum_ids
                ],
            )
            filters.append(is_wellbore_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellboreDataAPI(TypeAPI[WellboreData, WellboreDataApply, WellboreDataList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WellboreDataApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WellboreData,
            class_apply_type=WellboreDataApply,
            class_list=WellboreDataList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.drilling_reasons = WellboreDataDrillingReasonsAPI(client)
        self.facility_events = WellboreDataFacilityEventsAPI(client)
        self.facility_operators = WellboreDataFacilityOperatorsAPI(client)
        self.facility_specifications = WellboreDataFacilitySpecificationsAPI(client)
        self.facility_states = WellboreDataFacilityStatesAPI(client)
        self.geo_contexts = WellboreDataGeoContextsAPI(client)
        self.historical_interests = WellboreDataHistoricalInterestsAPI(client)
        self.name_aliases = WellboreDataNameAliasesAPI(client)
        self.technical_assurances = WellboreDataTechnicalAssurancesAPI(client)
        self.vertical_measurements = WellboreDataVerticalMeasurementsAPI(client)
        self.wellbore_costs = WellboreDataWellboreCostsAPI(client)

    def apply(
        self, wellbore_datum: WellboreDataApply | Sequence[WellboreDataApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) wellbore data.

        Note: This method iterates through all nodes linked to wellbore_datum and create them including the edges
        between the nodes. For example, if any of `drilling_reasons`, `facility_events`, `facility_operators`, `facility_specifications`, `facility_states`, `geo_contexts`, `historical_interests`, `name_aliases`, `technical_assurances`, `vertical_measurements` or `wellbore_costs` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            wellbore_datum: Wellbore datum or sequence of wellbore data to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new wellbore_datum:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import WellboreDataApply
                >>> client = OSDUClient()
                >>> wellbore_datum = WellboreDataApply(external_id="my_wellbore_datum", ...)
                >>> result = client.wellbore_data.apply(wellbore_datum)

        """
        if isinstance(wellbore_datum, WellboreDataApply):
            instances = wellbore_datum.to_instances_apply(self._view_by_write_class)
        else:
            instances = WellboreDataApplyList(wellbore_datum).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more wellbore datum.

        Args:
            external_id: External id of the wellbore datum to delete.
            space: The space where all the wellbore datum are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete wellbore_datum by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.wellbore_data.delete("my_wellbore_datum")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WellboreData:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WellboreDataList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> WellboreData | WellboreDataList:
        """Retrieve one or more wellbore data by id(s).

        Args:
            external_id: External id or list of external ids of the wellbore data.
            space: The space where all the wellbore data are located.

        Returns:
            The requested wellbore data.

        Examples:

            Retrieve wellbore_datum by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.retrieve("my_wellbore_datum")

        """
        if isinstance(external_id, str):
            wellbore_datum = self._retrieve((space, external_id))

            drilling_reason_edges = self.drilling_reasons.retrieve(external_id, space=space)
            wellbore_datum.drilling_reasons = [edge.end_node.external_id for edge in drilling_reason_edges]
            facility_event_edges = self.facility_events.retrieve(external_id, space=space)
            wellbore_datum.facility_events = [edge.end_node.external_id for edge in facility_event_edges]
            facility_operator_edges = self.facility_operators.retrieve(external_id, space=space)
            wellbore_datum.facility_operators = [edge.end_node.external_id for edge in facility_operator_edges]
            facility_specification_edges = self.facility_specifications.retrieve(external_id, space=space)
            wellbore_datum.facility_specifications = [
                edge.end_node.external_id for edge in facility_specification_edges
            ]
            facility_state_edges = self.facility_states.retrieve(external_id, space=space)
            wellbore_datum.facility_states = [edge.end_node.external_id for edge in facility_state_edges]
            geo_context_edges = self.geo_contexts.retrieve(external_id, space=space)
            wellbore_datum.geo_contexts = [edge.end_node.external_id for edge in geo_context_edges]
            historical_interest_edges = self.historical_interests.retrieve(external_id, space=space)
            wellbore_datum.historical_interests = [edge.end_node.external_id for edge in historical_interest_edges]
            name_alias_edges = self.name_aliases.retrieve(external_id, space=space)
            wellbore_datum.name_aliases = [edge.end_node.external_id for edge in name_alias_edges]
            technical_assurance_edges = self.technical_assurances.retrieve(external_id, space=space)
            wellbore_datum.technical_assurances = [edge.end_node.external_id for edge in technical_assurance_edges]
            vertical_measurement_edges = self.vertical_measurements.retrieve(external_id, space=space)
            wellbore_datum.vertical_measurements = [edge.end_node.external_id for edge in vertical_measurement_edges]
            wellbore_cost_edges = self.wellbore_costs.retrieve(external_id, space=space)
            wellbore_datum.wellbore_costs = [edge.end_node.external_id for edge in wellbore_cost_edges]

            return wellbore_datum
        else:
            wellbore_data = self._retrieve([(space, ext_id) for ext_id in external_id])

            drilling_reason_edges = self.drilling_reasons.retrieve(wellbore_data.as_node_ids())
            self._set_drilling_reasons(wellbore_data, drilling_reason_edges)
            facility_event_edges = self.facility_events.retrieve(wellbore_data.as_node_ids())
            self._set_facility_events(wellbore_data, facility_event_edges)
            facility_operator_edges = self.facility_operators.retrieve(wellbore_data.as_node_ids())
            self._set_facility_operators(wellbore_data, facility_operator_edges)
            facility_specification_edges = self.facility_specifications.retrieve(wellbore_data.as_node_ids())
            self._set_facility_specifications(wellbore_data, facility_specification_edges)
            facility_state_edges = self.facility_states.retrieve(wellbore_data.as_node_ids())
            self._set_facility_states(wellbore_data, facility_state_edges)
            geo_context_edges = self.geo_contexts.retrieve(wellbore_data.as_node_ids())
            self._set_geo_contexts(wellbore_data, geo_context_edges)
            historical_interest_edges = self.historical_interests.retrieve(wellbore_data.as_node_ids())
            self._set_historical_interests(wellbore_data, historical_interest_edges)
            name_alias_edges = self.name_aliases.retrieve(wellbore_data.as_node_ids())
            self._set_name_aliases(wellbore_data, name_alias_edges)
            technical_assurance_edges = self.technical_assurances.retrieve(wellbore_data.as_node_ids())
            self._set_technical_assurances(wellbore_data, technical_assurance_edges)
            vertical_measurement_edges = self.vertical_measurements.retrieve(wellbore_data.as_node_ids())
            self._set_vertical_measurements(wellbore_data, vertical_measurement_edges)
            wellbore_cost_edges = self.wellbore_costs.retrieve(wellbore_data.as_node_ids())
            self._set_wellbore_costs(wellbore_data, wellbore_cost_edges)

            return wellbore_data

    def search(
        self,
        query: str,
        properties: WellboreDataTextFields | Sequence[WellboreDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        facility_description: str | list[str] | None = None,
        facility_description_prefix: str | None = None,
        facility_id: str | list[str] | None = None,
        facility_id_prefix: str | None = None,
        facility_name: str | list[str] | None = None,
        facility_name_prefix: str | None = None,
        facility_type_id: str | list[str] | None = None,
        facility_type_id_prefix: str | None = None,
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellboreDataList:
        """Search wellbore data

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            business_intention_id: The business intention id to filter on.
            business_intention_id_prefix: The prefix of the business intention id to filter on.
            condition_id: The condition id to filter on.
            condition_id_prefix: The prefix of the condition id to filter on.
            current_operator_id: The current operator id to filter on.
            current_operator_id_prefix: The prefix of the current operator id to filter on.
            data_source_organisation_id: The data source organisation id to filter on.
            data_source_organisation_id_prefix: The prefix of the data source organisation id to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
            definitive_trajectory_id: The definitive trajectory id to filter on.
            definitive_trajectory_id_prefix: The prefix of the definitive trajectory id to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            facility_description: The facility description to filter on.
            facility_description_prefix: The prefix of the facility description to filter on.
            facility_id: The facility id to filter on.
            facility_id_prefix: The prefix of the facility id to filter on.
            facility_name: The facility name to filter on.
            facility_name_prefix: The prefix of the facility name to filter on.
            facility_type_id: The facility type id to filter on.
            facility_type_id_prefix: The prefix of the facility type id to filter on.
            fluid_direction_id: The fluid direction id to filter on.
            fluid_direction_id_prefix: The prefix of the fluid direction id to filter on.
            formation_name_at_total_depth: The formation name at total depth to filter on.
            formation_name_at_total_depth_prefix: The prefix of the formation name at total depth to filter on.
            geographic_bottom_hole_location: The geographic bottom hole location to filter on.
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            kick_off_wellbore: The kick off wellbore to filter on.
            kick_off_wellbore_prefix: The prefix of the kick off wellbore to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
            primary_product_type_id: The primary product type id to filter on.
            primary_product_type_id_prefix: The prefix of the primary product type id to filter on.
            projected_bottom_hole_location: The projected bottom hole location to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            secondary_product_type_id: The secondary product type id to filter on.
            secondary_product_type_id_prefix: The prefix of the secondary product type id to filter on.
            min_sequence_number: The minimum value of the sequence number to filter on.
            max_sequence_number: The maximum value of the sequence number to filter on.
            show_product_type_id: The show product type id to filter on.
            show_product_type_id_prefix: The prefix of the show product type id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            target_formation: The target formation to filter on.
            target_formation_prefix: The prefix of the target formation to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            tertiary_product_type_id: The tertiary product type id to filter on.
            tertiary_product_type_id_prefix: The prefix of the tertiary product type id to filter on.
            trajectory_type_id: The trajectory type id to filter on.
            trajectory_type_id_prefix: The prefix of the trajectory type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            well_id: The well id to filter on.
            well_id_prefix: The prefix of the well id to filter on.
            wellbore_reason_id: The wellbore reason id to filter on.
            wellbore_reason_id_prefix: The prefix of the wellbore reason id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results wellbore data matching the query.

        Examples:

           Search for 'my_wellbore_datum' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_data = client.wellbore_data.search('my_wellbore_datum')

        """
        filter_ = _create_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
            definitive_trajectory_id,
            definitive_trajectory_id_prefix,
            existence_kind,
            existence_kind_prefix,
            facility_description,
            facility_description_prefix,
            facility_id,
            facility_id_prefix,
            facility_name,
            facility_name_prefix,
            facility_type_id,
            facility_type_id_prefix,
            fluid_direction_id,
            fluid_direction_id_prefix,
            formation_name_at_total_depth,
            formation_name_at_total_depth_prefix,
            geographic_bottom_hole_location,
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            kick_off_wellbore,
            kick_off_wellbore_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
            primary_product_type_id,
            primary_product_type_id_prefix,
            projected_bottom_hole_location,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            role_id,
            role_id_prefix,
            secondary_product_type_id,
            secondary_product_type_id_prefix,
            min_sequence_number,
            max_sequence_number,
            show_product_type_id,
            show_product_type_id_prefix,
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            target_formation,
            target_formation_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            tertiary_product_type_id,
            tertiary_product_type_id_prefix,
            trajectory_type_id,
            trajectory_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            well_id,
            well_id_prefix,
            wellbore_reason_id,
            wellbore_reason_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WELLBOREDATA_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellboreDataFields | Sequence[WellboreDataFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WellboreDataTextFields | Sequence[WellboreDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        facility_description: str | list[str] | None = None,
        facility_description_prefix: str | None = None,
        facility_id: str | list[str] | None = None,
        facility_id_prefix: str | None = None,
        facility_name: str | list[str] | None = None,
        facility_name_prefix: str | None = None,
        facility_type_id: str | list[str] | None = None,
        facility_type_id_prefix: str | None = None,
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
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
        property: WellboreDataFields | Sequence[WellboreDataFields] | None = None,
        group_by: WellboreDataFields | Sequence[WellboreDataFields] = None,
        query: str | None = None,
        search_properties: WellboreDataTextFields | Sequence[WellboreDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        facility_description: str | list[str] | None = None,
        facility_description_prefix: str | None = None,
        facility_id: str | list[str] | None = None,
        facility_id_prefix: str | None = None,
        facility_name: str | list[str] | None = None,
        facility_name_prefix: str | None = None,
        facility_type_id: str | list[str] | None = None,
        facility_type_id_prefix: str | None = None,
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
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
        property: WellboreDataFields | Sequence[WellboreDataFields] | None = None,
        group_by: WellboreDataFields | Sequence[WellboreDataFields] | None = None,
        query: str | None = None,
        search_property: WellboreDataTextFields | Sequence[WellboreDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        facility_description: str | list[str] | None = None,
        facility_description_prefix: str | None = None,
        facility_id: str | list[str] | None = None,
        facility_id_prefix: str | None = None,
        facility_name: str | list[str] | None = None,
        facility_name_prefix: str | None = None,
        facility_type_id: str | list[str] | None = None,
        facility_type_id_prefix: str | None = None,
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across wellbore data

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            business_intention_id: The business intention id to filter on.
            business_intention_id_prefix: The prefix of the business intention id to filter on.
            condition_id: The condition id to filter on.
            condition_id_prefix: The prefix of the condition id to filter on.
            current_operator_id: The current operator id to filter on.
            current_operator_id_prefix: The prefix of the current operator id to filter on.
            data_source_organisation_id: The data source organisation id to filter on.
            data_source_organisation_id_prefix: The prefix of the data source organisation id to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
            definitive_trajectory_id: The definitive trajectory id to filter on.
            definitive_trajectory_id_prefix: The prefix of the definitive trajectory id to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            facility_description: The facility description to filter on.
            facility_description_prefix: The prefix of the facility description to filter on.
            facility_id: The facility id to filter on.
            facility_id_prefix: The prefix of the facility id to filter on.
            facility_name: The facility name to filter on.
            facility_name_prefix: The prefix of the facility name to filter on.
            facility_type_id: The facility type id to filter on.
            facility_type_id_prefix: The prefix of the facility type id to filter on.
            fluid_direction_id: The fluid direction id to filter on.
            fluid_direction_id_prefix: The prefix of the fluid direction id to filter on.
            formation_name_at_total_depth: The formation name at total depth to filter on.
            formation_name_at_total_depth_prefix: The prefix of the formation name at total depth to filter on.
            geographic_bottom_hole_location: The geographic bottom hole location to filter on.
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            kick_off_wellbore: The kick off wellbore to filter on.
            kick_off_wellbore_prefix: The prefix of the kick off wellbore to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
            primary_product_type_id: The primary product type id to filter on.
            primary_product_type_id_prefix: The prefix of the primary product type id to filter on.
            projected_bottom_hole_location: The projected bottom hole location to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            secondary_product_type_id: The secondary product type id to filter on.
            secondary_product_type_id_prefix: The prefix of the secondary product type id to filter on.
            min_sequence_number: The minimum value of the sequence number to filter on.
            max_sequence_number: The maximum value of the sequence number to filter on.
            show_product_type_id: The show product type id to filter on.
            show_product_type_id_prefix: The prefix of the show product type id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            target_formation: The target formation to filter on.
            target_formation_prefix: The prefix of the target formation to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            tertiary_product_type_id: The tertiary product type id to filter on.
            tertiary_product_type_id_prefix: The prefix of the tertiary product type id to filter on.
            trajectory_type_id: The trajectory type id to filter on.
            trajectory_type_id_prefix: The prefix of the trajectory type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            well_id: The well id to filter on.
            well_id_prefix: The prefix of the well id to filter on.
            wellbore_reason_id: The wellbore reason id to filter on.
            wellbore_reason_id_prefix: The prefix of the wellbore reason id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count wellbore data in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.wellbore_data.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
            definitive_trajectory_id,
            definitive_trajectory_id_prefix,
            existence_kind,
            existence_kind_prefix,
            facility_description,
            facility_description_prefix,
            facility_id,
            facility_id_prefix,
            facility_name,
            facility_name_prefix,
            facility_type_id,
            facility_type_id_prefix,
            fluid_direction_id,
            fluid_direction_id_prefix,
            formation_name_at_total_depth,
            formation_name_at_total_depth_prefix,
            geographic_bottom_hole_location,
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            kick_off_wellbore,
            kick_off_wellbore_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
            primary_product_type_id,
            primary_product_type_id_prefix,
            projected_bottom_hole_location,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            role_id,
            role_id_prefix,
            secondary_product_type_id,
            secondary_product_type_id_prefix,
            min_sequence_number,
            max_sequence_number,
            show_product_type_id,
            show_product_type_id_prefix,
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            target_formation,
            target_formation_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            tertiary_product_type_id,
            tertiary_product_type_id_prefix,
            trajectory_type_id,
            trajectory_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            well_id,
            well_id_prefix,
            wellbore_reason_id,
            wellbore_reason_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WELLBOREDATA_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WellboreDataFields,
        interval: float,
        query: str | None = None,
        search_property: WellboreDataTextFields | Sequence[WellboreDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        facility_description: str | list[str] | None = None,
        facility_description_prefix: str | None = None,
        facility_id: str | list[str] | None = None,
        facility_id_prefix: str | None = None,
        facility_name: str | list[str] | None = None,
        facility_name_prefix: str | None = None,
        facility_type_id: str | list[str] | None = None,
        facility_type_id_prefix: str | None = None,
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for wellbore data

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            business_intention_id: The business intention id to filter on.
            business_intention_id_prefix: The prefix of the business intention id to filter on.
            condition_id: The condition id to filter on.
            condition_id_prefix: The prefix of the condition id to filter on.
            current_operator_id: The current operator id to filter on.
            current_operator_id_prefix: The prefix of the current operator id to filter on.
            data_source_organisation_id: The data source organisation id to filter on.
            data_source_organisation_id_prefix: The prefix of the data source organisation id to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
            definitive_trajectory_id: The definitive trajectory id to filter on.
            definitive_trajectory_id_prefix: The prefix of the definitive trajectory id to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            facility_description: The facility description to filter on.
            facility_description_prefix: The prefix of the facility description to filter on.
            facility_id: The facility id to filter on.
            facility_id_prefix: The prefix of the facility id to filter on.
            facility_name: The facility name to filter on.
            facility_name_prefix: The prefix of the facility name to filter on.
            facility_type_id: The facility type id to filter on.
            facility_type_id_prefix: The prefix of the facility type id to filter on.
            fluid_direction_id: The fluid direction id to filter on.
            fluid_direction_id_prefix: The prefix of the fluid direction id to filter on.
            formation_name_at_total_depth: The formation name at total depth to filter on.
            formation_name_at_total_depth_prefix: The prefix of the formation name at total depth to filter on.
            geographic_bottom_hole_location: The geographic bottom hole location to filter on.
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            kick_off_wellbore: The kick off wellbore to filter on.
            kick_off_wellbore_prefix: The prefix of the kick off wellbore to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
            primary_product_type_id: The primary product type id to filter on.
            primary_product_type_id_prefix: The prefix of the primary product type id to filter on.
            projected_bottom_hole_location: The projected bottom hole location to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            secondary_product_type_id: The secondary product type id to filter on.
            secondary_product_type_id_prefix: The prefix of the secondary product type id to filter on.
            min_sequence_number: The minimum value of the sequence number to filter on.
            max_sequence_number: The maximum value of the sequence number to filter on.
            show_product_type_id: The show product type id to filter on.
            show_product_type_id_prefix: The prefix of the show product type id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            target_formation: The target formation to filter on.
            target_formation_prefix: The prefix of the target formation to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            tertiary_product_type_id: The tertiary product type id to filter on.
            tertiary_product_type_id_prefix: The prefix of the tertiary product type id to filter on.
            trajectory_type_id: The trajectory type id to filter on.
            trajectory_type_id_prefix: The prefix of the trajectory type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            well_id: The well id to filter on.
            well_id_prefix: The prefix of the well id to filter on.
            wellbore_reason_id: The wellbore reason id to filter on.
            wellbore_reason_id_prefix: The prefix of the wellbore reason id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `drilling_reasons`, `facility_events`, `facility_operators`, `facility_specifications`, `facility_states`, `geo_contexts`, `historical_interests`, `name_aliases`, `technical_assurances`, `vertical_measurements` or `wellbore_costs` external ids for the wellbore data. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
            definitive_trajectory_id,
            definitive_trajectory_id_prefix,
            existence_kind,
            existence_kind_prefix,
            facility_description,
            facility_description_prefix,
            facility_id,
            facility_id_prefix,
            facility_name,
            facility_name_prefix,
            facility_type_id,
            facility_type_id_prefix,
            fluid_direction_id,
            fluid_direction_id_prefix,
            formation_name_at_total_depth,
            formation_name_at_total_depth_prefix,
            geographic_bottom_hole_location,
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            kick_off_wellbore,
            kick_off_wellbore_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
            primary_product_type_id,
            primary_product_type_id_prefix,
            projected_bottom_hole_location,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            role_id,
            role_id_prefix,
            secondary_product_type_id,
            secondary_product_type_id_prefix,
            min_sequence_number,
            max_sequence_number,
            show_product_type_id,
            show_product_type_id_prefix,
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            target_formation,
            target_formation_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            tertiary_product_type_id,
            tertiary_product_type_id_prefix,
            trajectory_type_id,
            trajectory_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            well_id,
            well_id_prefix,
            wellbore_reason_id,
            wellbore_reason_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WELLBOREDATA_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
        existence_kind: str | list[str] | None = None,
        existence_kind_prefix: str | None = None,
        facility_description: str | list[str] | None = None,
        facility_description_prefix: str | None = None,
        facility_id: str | list[str] | None = None,
        facility_id_prefix: str | None = None,
        facility_name: str | list[str] | None = None,
        facility_name_prefix: str | None = None,
        facility_type_id: str | list[str] | None = None,
        facility_type_id_prefix: str | None = None,
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resource_curation_status: str | list[str] | None = None,
        resource_curation_status_prefix: str | None = None,
        resource_home_region_id: str | list[str] | None = None,
        resource_home_region_id_prefix: str | None = None,
        resource_lifecycle_status: str | list[str] | None = None,
        resource_lifecycle_status_prefix: str | None = None,
        resource_security_classification: str | list[str] | None = None,
        resource_security_classification_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WellboreDataList:
        """List/filter wellbore data

        Args:
            business_intention_id: The business intention id to filter on.
            business_intention_id_prefix: The prefix of the business intention id to filter on.
            condition_id: The condition id to filter on.
            condition_id_prefix: The prefix of the condition id to filter on.
            current_operator_id: The current operator id to filter on.
            current_operator_id_prefix: The prefix of the current operator id to filter on.
            data_source_organisation_id: The data source organisation id to filter on.
            data_source_organisation_id_prefix: The prefix of the data source organisation id to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
            definitive_trajectory_id: The definitive trajectory id to filter on.
            definitive_trajectory_id_prefix: The prefix of the definitive trajectory id to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            facility_description: The facility description to filter on.
            facility_description_prefix: The prefix of the facility description to filter on.
            facility_id: The facility id to filter on.
            facility_id_prefix: The prefix of the facility id to filter on.
            facility_name: The facility name to filter on.
            facility_name_prefix: The prefix of the facility name to filter on.
            facility_type_id: The facility type id to filter on.
            facility_type_id_prefix: The prefix of the facility type id to filter on.
            fluid_direction_id: The fluid direction id to filter on.
            fluid_direction_id_prefix: The prefix of the fluid direction id to filter on.
            formation_name_at_total_depth: The formation name at total depth to filter on.
            formation_name_at_total_depth_prefix: The prefix of the formation name at total depth to filter on.
            geographic_bottom_hole_location: The geographic bottom hole location to filter on.
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            kick_off_wellbore: The kick off wellbore to filter on.
            kick_off_wellbore_prefix: The prefix of the kick off wellbore to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
            primary_product_type_id: The primary product type id to filter on.
            primary_product_type_id_prefix: The prefix of the primary product type id to filter on.
            projected_bottom_hole_location: The projected bottom hole location to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            secondary_product_type_id: The secondary product type id to filter on.
            secondary_product_type_id_prefix: The prefix of the secondary product type id to filter on.
            min_sequence_number: The minimum value of the sequence number to filter on.
            max_sequence_number: The maximum value of the sequence number to filter on.
            show_product_type_id: The show product type id to filter on.
            show_product_type_id_prefix: The prefix of the show product type id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            target_formation: The target formation to filter on.
            target_formation_prefix: The prefix of the target formation to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            tertiary_product_type_id: The tertiary product type id to filter on.
            tertiary_product_type_id_prefix: The prefix of the tertiary product type id to filter on.
            trajectory_type_id: The trajectory type id to filter on.
            trajectory_type_id_prefix: The prefix of the trajectory type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            well_id: The well id to filter on.
            well_id_prefix: The prefix of the well id to filter on.
            wellbore_reason_id: The wellbore reason id to filter on.
            wellbore_reason_id_prefix: The prefix of the wellbore reason id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `drilling_reasons`, `facility_events`, `facility_operators`, `facility_specifications`, `facility_states`, `geo_contexts`, `historical_interests`, `name_aliases`, `technical_assurances`, `vertical_measurements` or `wellbore_costs` external ids for the wellbore data. Defaults to True.

        Returns:
            List of requested wellbore data

        Examples:

            List wellbore data and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_data = client.wellbore_data.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
            definitive_trajectory_id,
            definitive_trajectory_id_prefix,
            existence_kind,
            existence_kind_prefix,
            facility_description,
            facility_description_prefix,
            facility_id,
            facility_id_prefix,
            facility_name,
            facility_name_prefix,
            facility_type_id,
            facility_type_id_prefix,
            fluid_direction_id,
            fluid_direction_id_prefix,
            formation_name_at_total_depth,
            formation_name_at_total_depth_prefix,
            geographic_bottom_hole_location,
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            kick_off_wellbore,
            kick_off_wellbore_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
            primary_product_type_id,
            primary_product_type_id_prefix,
            projected_bottom_hole_location,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            role_id,
            role_id_prefix,
            secondary_product_type_id,
            secondary_product_type_id_prefix,
            min_sequence_number,
            max_sequence_number,
            show_product_type_id,
            show_product_type_id_prefix,
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            target_formation,
            target_formation_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            tertiary_product_type_id,
            tertiary_product_type_id_prefix,
            trajectory_type_id,
            trajectory_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            well_id,
            well_id_prefix,
            wellbore_reason_id,
            wellbore_reason_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        wellbore_data = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            space_arg = {"space": space} if space else {}
            if len(ids := wellbore_data.as_node_ids()) > IN_FILTER_LIMIT:
                drilling_reason_edges = self.drilling_reasons.list(limit=-1, **space_arg)
            else:
                drilling_reason_edges = self.drilling_reasons.list(ids, limit=-1)
            self._set_drilling_reasons(wellbore_data, drilling_reason_edges)
            if len(ids := wellbore_data.as_node_ids()) > IN_FILTER_LIMIT:
                facility_event_edges = self.facility_events.list(limit=-1, **space_arg)
            else:
                facility_event_edges = self.facility_events.list(ids, limit=-1)
            self._set_facility_events(wellbore_data, facility_event_edges)
            if len(ids := wellbore_data.as_node_ids()) > IN_FILTER_LIMIT:
                facility_operator_edges = self.facility_operators.list(limit=-1, **space_arg)
            else:
                facility_operator_edges = self.facility_operators.list(ids, limit=-1)
            self._set_facility_operators(wellbore_data, facility_operator_edges)
            if len(ids := wellbore_data.as_node_ids()) > IN_FILTER_LIMIT:
                facility_specification_edges = self.facility_specifications.list(limit=-1, **space_arg)
            else:
                facility_specification_edges = self.facility_specifications.list(ids, limit=-1)
            self._set_facility_specifications(wellbore_data, facility_specification_edges)
            if len(ids := wellbore_data.as_node_ids()) > IN_FILTER_LIMIT:
                facility_state_edges = self.facility_states.list(limit=-1, **space_arg)
            else:
                facility_state_edges = self.facility_states.list(ids, limit=-1)
            self._set_facility_states(wellbore_data, facility_state_edges)
            if len(ids := wellbore_data.as_node_ids()) > IN_FILTER_LIMIT:
                geo_context_edges = self.geo_contexts.list(limit=-1, **space_arg)
            else:
                geo_context_edges = self.geo_contexts.list(ids, limit=-1)
            self._set_geo_contexts(wellbore_data, geo_context_edges)
            if len(ids := wellbore_data.as_node_ids()) > IN_FILTER_LIMIT:
                historical_interest_edges = self.historical_interests.list(limit=-1, **space_arg)
            else:
                historical_interest_edges = self.historical_interests.list(ids, limit=-1)
            self._set_historical_interests(wellbore_data, historical_interest_edges)
            if len(ids := wellbore_data.as_node_ids()) > IN_FILTER_LIMIT:
                name_alias_edges = self.name_aliases.list(limit=-1, **space_arg)
            else:
                name_alias_edges = self.name_aliases.list(ids, limit=-1)
            self._set_name_aliases(wellbore_data, name_alias_edges)
            if len(ids := wellbore_data.as_node_ids()) > IN_FILTER_LIMIT:
                technical_assurance_edges = self.technical_assurances.list(limit=-1, **space_arg)
            else:
                technical_assurance_edges = self.technical_assurances.list(ids, limit=-1)
            self._set_technical_assurances(wellbore_data, technical_assurance_edges)
            if len(ids := wellbore_data.as_node_ids()) > IN_FILTER_LIMIT:
                vertical_measurement_edges = self.vertical_measurements.list(limit=-1, **space_arg)
            else:
                vertical_measurement_edges = self.vertical_measurements.list(ids, limit=-1)
            self._set_vertical_measurements(wellbore_data, vertical_measurement_edges)
            if len(ids := wellbore_data.as_node_ids()) > IN_FILTER_LIMIT:
                wellbore_cost_edges = self.wellbore_costs.list(limit=-1, **space_arg)
            else:
                wellbore_cost_edges = self.wellbore_costs.list(ids, limit=-1)
            self._set_wellbore_costs(wellbore_data, wellbore_cost_edges)

        return wellbore_data

    @staticmethod
    def _set_drilling_reasons(wellbore_data: Sequence[WellboreData], drilling_reason_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in drilling_reason_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_datum in wellbore_data:
            node_id = wellbore_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_datum.drilling_reasons = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_facility_events(wellbore_data: Sequence[WellboreData], facility_event_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in facility_event_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_datum in wellbore_data:
            node_id = wellbore_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_datum.facility_events = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_facility_operators(wellbore_data: Sequence[WellboreData], facility_operator_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in facility_operator_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_datum in wellbore_data:
            node_id = wellbore_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_datum.facility_operators = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_facility_specifications(
        wellbore_data: Sequence[WellboreData], facility_specification_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in facility_specification_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_datum in wellbore_data:
            node_id = wellbore_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_datum.facility_specifications = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_facility_states(wellbore_data: Sequence[WellboreData], facility_state_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in facility_state_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_datum in wellbore_data:
            node_id = wellbore_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_datum.facility_states = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_geo_contexts(wellbore_data: Sequence[WellboreData], geo_context_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in geo_context_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_datum in wellbore_data:
            node_id = wellbore_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_datum.geo_contexts = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_historical_interests(wellbore_data: Sequence[WellboreData], historical_interest_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in historical_interest_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_datum in wellbore_data:
            node_id = wellbore_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_datum.historical_interests = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_name_aliases(wellbore_data: Sequence[WellboreData], name_alias_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in name_alias_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_datum in wellbore_data:
            node_id = wellbore_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_datum.name_aliases = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_technical_assurances(wellbore_data: Sequence[WellboreData], technical_assurance_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in technical_assurance_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_datum in wellbore_data:
            node_id = wellbore_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_datum.technical_assurances = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_vertical_measurements(
        wellbore_data: Sequence[WellboreData], vertical_measurement_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in vertical_measurement_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_datum in wellbore_data:
            node_id = wellbore_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_datum.vertical_measurements = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_wellbore_costs(wellbore_data: Sequence[WellboreData], wellbore_cost_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in wellbore_cost_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wellbore_datum in wellbore_data:
            node_id = wellbore_datum.id_tuple()
            if node_id in edges_by_start_node:
                wellbore_datum.wellbore_costs = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    business_intention_id: str | list[str] | None = None,
    business_intention_id_prefix: str | None = None,
    condition_id: str | list[str] | None = None,
    condition_id_prefix: str | None = None,
    current_operator_id: str | list[str] | None = None,
    current_operator_id_prefix: str | None = None,
    data_source_organisation_id: str | list[str] | None = None,
    data_source_organisation_id_prefix: str | None = None,
    default_vertical_measurement_id: str | list[str] | None = None,
    default_vertical_measurement_id_prefix: str | None = None,
    definitive_trajectory_id: str | list[str] | None = None,
    definitive_trajectory_id_prefix: str | None = None,
    existence_kind: str | list[str] | None = None,
    existence_kind_prefix: str | None = None,
    facility_description: str | list[str] | None = None,
    facility_description_prefix: str | None = None,
    facility_id: str | list[str] | None = None,
    facility_id_prefix: str | None = None,
    facility_name: str | list[str] | None = None,
    facility_name_prefix: str | None = None,
    facility_type_id: str | list[str] | None = None,
    facility_type_id_prefix: str | None = None,
    fluid_direction_id: str | list[str] | None = None,
    fluid_direction_id_prefix: str | None = None,
    formation_name_at_total_depth: str | list[str] | None = None,
    formation_name_at_total_depth_prefix: str | None = None,
    geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    initial_operator_id: str | list[str] | None = None,
    initial_operator_id_prefix: str | None = None,
    interest_type_id: str | list[str] | None = None,
    interest_type_id_prefix: str | None = None,
    kick_off_wellbore: str | list[str] | None = None,
    kick_off_wellbore_prefix: str | None = None,
    operating_environment_id: str | list[str] | None = None,
    operating_environment_id_prefix: str | None = None,
    outcome_id: str | list[str] | None = None,
    outcome_id_prefix: str | None = None,
    primary_product_type_id: str | list[str] | None = None,
    primary_product_type_id_prefix: str | None = None,
    projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    resource_curation_status: str | list[str] | None = None,
    resource_curation_status_prefix: str | None = None,
    resource_home_region_id: str | list[str] | None = None,
    resource_home_region_id_prefix: str | None = None,
    resource_lifecycle_status: str | list[str] | None = None,
    resource_lifecycle_status_prefix: str | None = None,
    resource_security_classification: str | list[str] | None = None,
    resource_security_classification_prefix: str | None = None,
    role_id: str | list[str] | None = None,
    role_id_prefix: str | None = None,
    secondary_product_type_id: str | list[str] | None = None,
    secondary_product_type_id_prefix: str | None = None,
    min_sequence_number: int | None = None,
    max_sequence_number: int | None = None,
    show_product_type_id: str | list[str] | None = None,
    show_product_type_id_prefix: str | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
    spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    status_summary_id: str | list[str] | None = None,
    status_summary_id_prefix: str | None = None,
    target_formation: str | list[str] | None = None,
    target_formation_prefix: str | None = None,
    technical_assurance_type_id: str | list[str] | None = None,
    technical_assurance_type_id_prefix: str | None = None,
    tertiary_product_type_id: str | list[str] | None = None,
    tertiary_product_type_id_prefix: str | None = None,
    trajectory_type_id: str | list[str] | None = None,
    trajectory_type_id_prefix: str | None = None,
    version_creation_reason: str | list[str] | None = None,
    version_creation_reason_prefix: str | None = None,
    was_business_interest_financial_non_operated: bool | None = None,
    was_business_interest_financial_operated: bool | None = None,
    was_business_interest_obligatory: bool | None = None,
    was_business_interest_technical: bool | None = None,
    well_id: str | list[str] | None = None,
    well_id_prefix: str | None = None,
    wellbore_reason_id: str | list[str] | None = None,
    wellbore_reason_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if business_intention_id and isinstance(business_intention_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("BusinessIntentionID"), value=business_intention_id))
    if business_intention_id and isinstance(business_intention_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("BusinessIntentionID"), values=business_intention_id))
    if business_intention_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("BusinessIntentionID"), value=business_intention_id_prefix)
        )
    if condition_id and isinstance(condition_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ConditionID"), value=condition_id))
    if condition_id and isinstance(condition_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ConditionID"), values=condition_id))
    if condition_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ConditionID"), value=condition_id_prefix))
    if current_operator_id and isinstance(current_operator_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("CurrentOperatorID"), value=current_operator_id))
    if current_operator_id and isinstance(current_operator_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("CurrentOperatorID"), values=current_operator_id))
    if current_operator_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("CurrentOperatorID"), value=current_operator_id_prefix)
        )
    if data_source_organisation_id and isinstance(data_source_organisation_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("DataSourceOrganisationID"), value=data_source_organisation_id)
        )
    if data_source_organisation_id and isinstance(data_source_organisation_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("DataSourceOrganisationID"), values=data_source_organisation_id)
        )
    if data_source_organisation_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("DataSourceOrganisationID"), value=data_source_organisation_id_prefix
            )
        )
    if default_vertical_measurement_id and isinstance(default_vertical_measurement_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("DefaultVerticalMeasurementID"), value=default_vertical_measurement_id
            )
        )
    if default_vertical_measurement_id and isinstance(default_vertical_measurement_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("DefaultVerticalMeasurementID"), values=default_vertical_measurement_id
            )
        )
    if default_vertical_measurement_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("DefaultVerticalMeasurementID"), value=default_vertical_measurement_id_prefix
            )
        )
    if definitive_trajectory_id and isinstance(definitive_trajectory_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("DefinitiveTrajectoryID"), value=definitive_trajectory_id)
        )
    if definitive_trajectory_id and isinstance(definitive_trajectory_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("DefinitiveTrajectoryID"), values=definitive_trajectory_id)
        )
    if definitive_trajectory_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("DefinitiveTrajectoryID"), value=definitive_trajectory_id_prefix)
        )
    if existence_kind and isinstance(existence_kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ExistenceKind"), value=existence_kind))
    if existence_kind and isinstance(existence_kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ExistenceKind"), values=existence_kind))
    if existence_kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ExistenceKind"), value=existence_kind_prefix))
    if facility_description and isinstance(facility_description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityDescription"), value=facility_description))
    if facility_description and isinstance(facility_description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityDescription"), values=facility_description))
    if facility_description_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("FacilityDescription"), value=facility_description_prefix)
        )
    if facility_id and isinstance(facility_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityID"), value=facility_id))
    if facility_id and isinstance(facility_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityID"), values=facility_id))
    if facility_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("FacilityID"), value=facility_id_prefix))
    if facility_name and isinstance(facility_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityName"), value=facility_name))
    if facility_name and isinstance(facility_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityName"), values=facility_name))
    if facility_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("FacilityName"), value=facility_name_prefix))
    if facility_type_id and isinstance(facility_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityTypeID"), value=facility_type_id))
    if facility_type_id and isinstance(facility_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityTypeID"), values=facility_type_id))
    if facility_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("FacilityTypeID"), value=facility_type_id_prefix))
    if fluid_direction_id and isinstance(fluid_direction_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FluidDirectionID"), value=fluid_direction_id))
    if fluid_direction_id and isinstance(fluid_direction_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FluidDirectionID"), values=fluid_direction_id))
    if fluid_direction_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("FluidDirectionID"), value=fluid_direction_id_prefix))
    if formation_name_at_total_depth and isinstance(formation_name_at_total_depth, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("FormationNameAtTotalDepth"), value=formation_name_at_total_depth)
        )
    if formation_name_at_total_depth and isinstance(formation_name_at_total_depth, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("FormationNameAtTotalDepth"), values=formation_name_at_total_depth)
        )
    if formation_name_at_total_depth_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("FormationNameAtTotalDepth"), value=formation_name_at_total_depth_prefix
            )
        )
    if geographic_bottom_hole_location and isinstance(geographic_bottom_hole_location, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("GeographicBottomHoleLocation"),
                value={"space": "IntegrationTestsImmutable", "externalId": geographic_bottom_hole_location},
            )
        )
    if geographic_bottom_hole_location and isinstance(geographic_bottom_hole_location, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("GeographicBottomHoleLocation"),
                value={"space": geographic_bottom_hole_location[0], "externalId": geographic_bottom_hole_location[1]},
            )
        )
    if (
        geographic_bottom_hole_location
        and isinstance(geographic_bottom_hole_location, list)
        and isinstance(geographic_bottom_hole_location[0], str)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("GeographicBottomHoleLocation"),
                values=[
                    {"space": "IntegrationTestsImmutable", "externalId": item}
                    for item in geographic_bottom_hole_location
                ],
            )
        )
    if (
        geographic_bottom_hole_location
        and isinstance(geographic_bottom_hole_location, list)
        and isinstance(geographic_bottom_hole_location[0], tuple)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("GeographicBottomHoleLocation"),
                values=[{"space": item[0], "externalId": item[1]} for item in geographic_bottom_hole_location],
            )
        )
    if initial_operator_id and isinstance(initial_operator_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("InitialOperatorID"), value=initial_operator_id))
    if initial_operator_id and isinstance(initial_operator_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("InitialOperatorID"), values=initial_operator_id))
    if initial_operator_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("InitialOperatorID"), value=initial_operator_id_prefix)
        )
    if interest_type_id and isinstance(interest_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("InterestTypeID"), value=interest_type_id))
    if interest_type_id and isinstance(interest_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("InterestTypeID"), values=interest_type_id))
    if interest_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("InterestTypeID"), value=interest_type_id_prefix))
    if kick_off_wellbore and isinstance(kick_off_wellbore, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("KickOffWellbore"), value=kick_off_wellbore))
    if kick_off_wellbore and isinstance(kick_off_wellbore, list):
        filters.append(dm.filters.In(view_id.as_property_ref("KickOffWellbore"), values=kick_off_wellbore))
    if kick_off_wellbore_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("KickOffWellbore"), value=kick_off_wellbore_prefix))
    if operating_environment_id and isinstance(operating_environment_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("OperatingEnvironmentID"), value=operating_environment_id)
        )
    if operating_environment_id and isinstance(operating_environment_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("OperatingEnvironmentID"), values=operating_environment_id)
        )
    if operating_environment_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("OperatingEnvironmentID"), value=operating_environment_id_prefix)
        )
    if outcome_id and isinstance(outcome_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("OutcomeID"), value=outcome_id))
    if outcome_id and isinstance(outcome_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("OutcomeID"), values=outcome_id))
    if outcome_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("OutcomeID"), value=outcome_id_prefix))
    if primary_product_type_id and isinstance(primary_product_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("PrimaryProductTypeID"), value=primary_product_type_id)
        )
    if primary_product_type_id and isinstance(primary_product_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("PrimaryProductTypeID"), values=primary_product_type_id))
    if primary_product_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("PrimaryProductTypeID"), value=primary_product_type_id_prefix)
        )
    if projected_bottom_hole_location and isinstance(projected_bottom_hole_location, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("ProjectedBottomHoleLocation"),
                value={"space": "IntegrationTestsImmutable", "externalId": projected_bottom_hole_location},
            )
        )
    if projected_bottom_hole_location and isinstance(projected_bottom_hole_location, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("ProjectedBottomHoleLocation"),
                value={"space": projected_bottom_hole_location[0], "externalId": projected_bottom_hole_location[1]},
            )
        )
    if (
        projected_bottom_hole_location
        and isinstance(projected_bottom_hole_location, list)
        and isinstance(projected_bottom_hole_location[0], str)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("ProjectedBottomHoleLocation"),
                values=[
                    {"space": "IntegrationTestsImmutable", "externalId": item}
                    for item in projected_bottom_hole_location
                ],
            )
        )
    if (
        projected_bottom_hole_location
        and isinstance(projected_bottom_hole_location, list)
        and isinstance(projected_bottom_hole_location[0], tuple)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("ProjectedBottomHoleLocation"),
                values=[{"space": item[0], "externalId": item[1]} for item in projected_bottom_hole_location],
            )
        )
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
    if role_id and isinstance(role_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("RoleID"), value=role_id))
    if role_id and isinstance(role_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("RoleID"), values=role_id))
    if role_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("RoleID"), value=role_id_prefix))
    if secondary_product_type_id and isinstance(secondary_product_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("SecondaryProductTypeID"), value=secondary_product_type_id)
        )
    if secondary_product_type_id and isinstance(secondary_product_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("SecondaryProductTypeID"), values=secondary_product_type_id)
        )
    if secondary_product_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("SecondaryProductTypeID"), value=secondary_product_type_id_prefix)
        )
    if min_sequence_number or max_sequence_number:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("SequenceNumber"), gte=min_sequence_number, lte=max_sequence_number
            )
        )
    if show_product_type_id and isinstance(show_product_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ShowProductTypeID"), value=show_product_type_id))
    if show_product_type_id and isinstance(show_product_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ShowProductTypeID"), values=show_product_type_id))
    if show_product_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("ShowProductTypeID"), value=show_product_type_id_prefix)
        )
    if source and isinstance(source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Source"), value=source))
    if source and isinstance(source, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Source"), values=source))
    if source_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Source"), value=source_prefix))
    if spatial_location and isinstance(spatial_location, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialLocation"),
                value={"space": "IntegrationTestsImmutable", "externalId": spatial_location},
            )
        )
    if spatial_location and isinstance(spatial_location, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialLocation"),
                value={"space": spatial_location[0], "externalId": spatial_location[1]},
            )
        )
    if spatial_location and isinstance(spatial_location, list) and isinstance(spatial_location[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialLocation"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in spatial_location],
            )
        )
    if spatial_location and isinstance(spatial_location, list) and isinstance(spatial_location[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialLocation"),
                values=[{"space": item[0], "externalId": item[1]} for item in spatial_location],
            )
        )
    if status_summary_id and isinstance(status_summary_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("StatusSummaryID"), value=status_summary_id))
    if status_summary_id and isinstance(status_summary_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("StatusSummaryID"), values=status_summary_id))
    if status_summary_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("StatusSummaryID"), value=status_summary_id_prefix))
    if target_formation and isinstance(target_formation, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TargetFormation"), value=target_formation))
    if target_formation and isinstance(target_formation, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TargetFormation"), values=target_formation))
    if target_formation_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("TargetFormation"), value=target_formation_prefix))
    if technical_assurance_type_id and isinstance(technical_assurance_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("TechnicalAssuranceTypeID"), value=technical_assurance_type_id)
        )
    if technical_assurance_type_id and isinstance(technical_assurance_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("TechnicalAssuranceTypeID"), values=technical_assurance_type_id)
        )
    if technical_assurance_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("TechnicalAssuranceTypeID"), value=technical_assurance_type_id_prefix
            )
        )
    if tertiary_product_type_id and isinstance(tertiary_product_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("TertiaryProductTypeID"), value=tertiary_product_type_id)
        )
    if tertiary_product_type_id and isinstance(tertiary_product_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TertiaryProductTypeID"), values=tertiary_product_type_id))
    if tertiary_product_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TertiaryProductTypeID"), value=tertiary_product_type_id_prefix)
        )
    if trajectory_type_id and isinstance(trajectory_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TrajectoryTypeID"), value=trajectory_type_id))
    if trajectory_type_id and isinstance(trajectory_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TrajectoryTypeID"), values=trajectory_type_id))
    if trajectory_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("TrajectoryTypeID"), value=trajectory_type_id_prefix))
    if version_creation_reason and isinstance(version_creation_reason, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("VersionCreationReason"), value=version_creation_reason)
        )
    if version_creation_reason and isinstance(version_creation_reason, list):
        filters.append(dm.filters.In(view_id.as_property_ref("VersionCreationReason"), values=version_creation_reason))
    if version_creation_reason_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("VersionCreationReason"), value=version_creation_reason_prefix)
        )
    if was_business_interest_financial_non_operated and isinstance(was_business_interest_financial_non_operated, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("WasBusinessInterestFinancialNonOperated"),
                value=was_business_interest_financial_non_operated,
            )
        )
    if was_business_interest_financial_operated and isinstance(was_business_interest_financial_operated, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("WasBusinessInterestFinancialOperated"),
                value=was_business_interest_financial_operated,
            )
        )
    if was_business_interest_obligatory and isinstance(was_business_interest_obligatory, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("WasBusinessInterestObligatory"), value=was_business_interest_obligatory
            )
        )
    if was_business_interest_technical and isinstance(was_business_interest_technical, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("WasBusinessInterestTechnical"), value=was_business_interest_technical
            )
        )
    if well_id and isinstance(well_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("WellID"), value=well_id))
    if well_id and isinstance(well_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("WellID"), values=well_id))
    if well_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("WellID"), value=well_id_prefix))
    if wellbore_reason_id and isinstance(wellbore_reason_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("WellboreReasonID"), value=wellbore_reason_id))
    if wellbore_reason_id and isinstance(wellbore_reason_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("WellboreReasonID"), values=wellbore_reason_id))
    if wellbore_reason_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("WellboreReasonID"), value=wellbore_reason_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
