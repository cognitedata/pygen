from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    WellData,
    WellDataApply,
    WellDataList,
    WellDataApplyList,
    WellDataFields,
    WellDataTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._well_data import _WELLDATA_PROPERTIES_BY_FIELD


class WellDataFacilityEventsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.FacilityEvents"},
        )
        if isinstance(external_id, str):
            is_well_datum = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_well_datum)
            )

        else:
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_well_data))

    def list(
        self, well_datum_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.FacilityEvents"},
        )
        filters.append(is_edge_type)
        if well_datum_id:
            well_datum_ids = [well_datum_id] if isinstance(well_datum_id, str) else well_datum_id
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in well_datum_ids],
            )
            filters.append(is_well_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellDataFacilityOperatorsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.FacilityOperators"},
        )
        if isinstance(external_id, str):
            is_well_datum = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_well_datum)
            )

        else:
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_well_data))

    def list(
        self, well_datum_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.FacilityOperators"},
        )
        filters.append(is_edge_type)
        if well_datum_id:
            well_datum_ids = [well_datum_id] if isinstance(well_datum_id, str) else well_datum_id
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in well_datum_ids],
            )
            filters.append(is_well_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellDataFacilitySpecificationsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.FacilitySpecifications"},
        )
        if isinstance(external_id, str):
            is_well_datum = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_well_datum)
            )

        else:
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_well_data))

    def list(
        self, well_datum_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.FacilitySpecifications"},
        )
        filters.append(is_edge_type)
        if well_datum_id:
            well_datum_ids = [well_datum_id] if isinstance(well_datum_id, str) else well_datum_id
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in well_datum_ids],
            )
            filters.append(is_well_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellDataFacilityStatesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.FacilityStates"},
        )
        if isinstance(external_id, str):
            is_well_datum = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_well_datum)
            )

        else:
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_well_data))

    def list(
        self, well_datum_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.FacilityStates"},
        )
        filters.append(is_edge_type)
        if well_datum_id:
            well_datum_ids = [well_datum_id] if isinstance(well_datum_id, str) else well_datum_id
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in well_datum_ids],
            )
            filters.append(is_well_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellDataGeoContextsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.GeoContexts"},
        )
        if isinstance(external_id, str):
            is_well_datum = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_well_datum)
            )

        else:
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_well_data))

    def list(
        self, well_datum_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.GeoContexts"},
        )
        filters.append(is_edge_type)
        if well_datum_id:
            well_datum_ids = [well_datum_id] if isinstance(well_datum_id, str) else well_datum_id
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in well_datum_ids],
            )
            filters.append(is_well_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellDataHistoricalInterestsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.HistoricalInterests"},
        )
        if isinstance(external_id, str):
            is_well_datum = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_well_datum)
            )

        else:
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_well_data))

    def list(
        self, well_datum_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.HistoricalInterests"},
        )
        filters.append(is_edge_type)
        if well_datum_id:
            well_datum_ids = [well_datum_id] if isinstance(well_datum_id, str) else well_datum_id
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in well_datum_ids],
            )
            filters.append(is_well_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellDataNameAliasesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.NameAliases"},
        )
        if isinstance(external_id, str):
            is_well_datum = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_well_datum)
            )

        else:
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_well_data))

    def list(
        self, well_datum_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.NameAliases"},
        )
        filters.append(is_edge_type)
        if well_datum_id:
            well_datum_ids = [well_datum_id] if isinstance(well_datum_id, str) else well_datum_id
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in well_datum_ids],
            )
            filters.append(is_well_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellDataTechnicalAssurancesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.TechnicalAssurances"},
        )
        if isinstance(external_id, str):
            is_well_datum = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_well_datum)
            )

        else:
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_well_data))

    def list(
        self, well_datum_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.TechnicalAssurances"},
        )
        filters.append(is_edge_type)
        if well_datum_id:
            well_datum_ids = [well_datum_id] if isinstance(well_datum_id, str) else well_datum_id
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in well_datum_ids],
            )
            filters.append(is_well_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellDataVerticalMeasurementsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.VerticalMeasurements"},
        )
        if isinstance(external_id, str):
            is_well_datum = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_well_datum)
            )

        else:
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_well_data))

    def list(
        self, well_datum_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WellData.VerticalMeasurements"},
        )
        filters.append(is_edge_type)
        if well_datum_id:
            well_datum_ids = [well_datum_id] if isinstance(well_datum_id, str) else well_datum_id
            is_well_data = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in well_datum_ids],
            )
            filters.append(is_well_data)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WellDataAPI(TypeAPI[WellData, WellDataApply, WellDataList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WellDataApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WellData,
            class_apply_type=WellDataApply,
            class_list=WellDataList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.facility_events = WellDataFacilityEventsAPI(client)
        self.facility_operators = WellDataFacilityOperatorsAPI(client)
        self.facility_specifications = WellDataFacilitySpecificationsAPI(client)
        self.facility_states = WellDataFacilityStatesAPI(client)
        self.geo_contexts = WellDataGeoContextsAPI(client)
        self.historical_interests = WellDataHistoricalInterestsAPI(client)
        self.name_aliases = WellDataNameAliasesAPI(client)
        self.technical_assurances = WellDataTechnicalAssurancesAPI(client)
        self.vertical_measurements = WellDataVerticalMeasurementsAPI(client)

    def apply(
        self, well_datum: WellDataApply | Sequence[WellDataApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) well data.

        Note: This method iterates through all nodes linked to well_datum and create them including the edges
        between the nodes. For example, if any of `facility_events`, `facility_operators`, `facility_specifications`, `facility_states`, `geo_contexts`, `historical_interests`, `name_aliases`, `technical_assurances` or `vertical_measurements` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            well_datum: Well datum or sequence of well data to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new well_datum:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import WellDataApply
                >>> client = OSDUClient()
                >>> well_datum = WellDataApply(external_id="my_well_datum", ...)
                >>> result = client.well_data.apply(well_datum)

        """
        if isinstance(well_datum, WellDataApply):
            instances = well_datum.to_instances_apply(self._view_by_write_class)
        else:
            instances = WellDataApplyList(well_datum).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more well datum.

        Args:
            external_id: External id of the well datum to delete.
            space: The space where all the well datum are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete well_datum by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.well_data.delete("my_well_datum")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WellData:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WellDataList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> WellData | WellDataList:
        """Retrieve one or more well data by id(s).

        Args:
            external_id: External id or list of external ids of the well data.
            space: The space where all the well data are located.

        Returns:
            The requested well data.

        Examples:

            Retrieve well_datum by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> well_datum = client.well_data.retrieve("my_well_datum")

        """
        if isinstance(external_id, str):
            well_datum = self._retrieve((space, external_id))

            facility_event_edges = self.facility_events.retrieve(external_id)
            well_datum.facility_events = [edge.end_node.external_id for edge in facility_event_edges]
            facility_operator_edges = self.facility_operators.retrieve(external_id)
            well_datum.facility_operators = [edge.end_node.external_id for edge in facility_operator_edges]
            facility_specification_edges = self.facility_specifications.retrieve(external_id)
            well_datum.facility_specifications = [edge.end_node.external_id for edge in facility_specification_edges]
            facility_state_edges = self.facility_states.retrieve(external_id)
            well_datum.facility_states = [edge.end_node.external_id for edge in facility_state_edges]
            geo_context_edges = self.geo_contexts.retrieve(external_id)
            well_datum.geo_contexts = [edge.end_node.external_id for edge in geo_context_edges]
            historical_interest_edges = self.historical_interests.retrieve(external_id)
            well_datum.historical_interests = [edge.end_node.external_id for edge in historical_interest_edges]
            name_alias_edges = self.name_aliases.retrieve(external_id)
            well_datum.name_aliases = [edge.end_node.external_id for edge in name_alias_edges]
            technical_assurance_edges = self.technical_assurances.retrieve(external_id)
            well_datum.technical_assurances = [edge.end_node.external_id for edge in technical_assurance_edges]
            vertical_measurement_edges = self.vertical_measurements.retrieve(external_id)
            well_datum.vertical_measurements = [edge.end_node.external_id for edge in vertical_measurement_edges]

            return well_datum
        else:
            well_data = self._retrieve([(space, ext_id) for ext_id in external_id])

            facility_event_edges = self.facility_events.retrieve(external_id)
            self._set_facility_events(well_data, facility_event_edges)
            facility_operator_edges = self.facility_operators.retrieve(external_id)
            self._set_facility_operators(well_data, facility_operator_edges)
            facility_specification_edges = self.facility_specifications.retrieve(external_id)
            self._set_facility_specifications(well_data, facility_specification_edges)
            facility_state_edges = self.facility_states.retrieve(external_id)
            self._set_facility_states(well_data, facility_state_edges)
            geo_context_edges = self.geo_contexts.retrieve(external_id)
            self._set_geo_contexts(well_data, geo_context_edges)
            historical_interest_edges = self.historical_interests.retrieve(external_id)
            self._set_historical_interests(well_data, historical_interest_edges)
            name_alias_edges = self.name_aliases.retrieve(external_id)
            self._set_name_aliases(well_data, name_alias_edges)
            technical_assurance_edges = self.technical_assurances.retrieve(external_id)
            self._set_technical_assurances(well_data, technical_assurance_edges)
            vertical_measurement_edges = self.vertical_measurements.retrieve(external_id)
            self._set_vertical_measurements(well_data, vertical_measurement_edges)

            return well_data

    def search(
        self,
        query: str,
        properties: WellDataTextFields | Sequence[WellDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_crsid: str | list[str] | None = None,
        default_vertical_crsid_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
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
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
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
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellDataList:
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
            default_vertical_crsid,
            default_vertical_crsid_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
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
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
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
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WELLDATA_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellDataFields | Sequence[WellDataFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WellDataTextFields | Sequence[WellDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_crsid: str | list[str] | None = None,
        default_vertical_crsid_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
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
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
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
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
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
        property: WellDataFields | Sequence[WellDataFields] | None = None,
        group_by: WellDataFields | Sequence[WellDataFields] = None,
        query: str | None = None,
        search_properties: WellDataTextFields | Sequence[WellDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_crsid: str | list[str] | None = None,
        default_vertical_crsid_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
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
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
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
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
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
        property: WellDataFields | Sequence[WellDataFields] | None = None,
        group_by: WellDataFields | Sequence[WellDataFields] | None = None,
        query: str | None = None,
        search_property: WellDataTextFields | Sequence[WellDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_crsid: str | list[str] | None = None,
        default_vertical_crsid_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
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
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
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
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
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
            default_vertical_crsid,
            default_vertical_crsid_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
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
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
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
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WELLDATA_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WellDataFields,
        interval: float,
        query: str | None = None,
        search_property: WellDataTextFields | Sequence[WellDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_crsid: str | list[str] | None = None,
        default_vertical_crsid_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
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
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
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
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
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
            default_vertical_crsid,
            default_vertical_crsid_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
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
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
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
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WELLDATA_PROPERTIES_BY_FIELD,
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
        default_vertical_crsid: str | list[str] | None = None,
        default_vertical_crsid_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
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
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
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
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WellDataList:
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
            default_vertical_crsid,
            default_vertical_crsid_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
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
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
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
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            external_id_prefix,
            space,
            filter,
        )

        well_data = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := well_data.as_external_ids()) > IN_FILTER_LIMIT:
                facility_event_edges = self.facility_events.list(limit=-1)
            else:
                facility_event_edges = self.facility_events.list(external_ids, limit=-1)
            self._set_facility_events(well_data, facility_event_edges)
            if len(external_ids := well_data.as_external_ids()) > IN_FILTER_LIMIT:
                facility_operator_edges = self.facility_operators.list(limit=-1)
            else:
                facility_operator_edges = self.facility_operators.list(external_ids, limit=-1)
            self._set_facility_operators(well_data, facility_operator_edges)
            if len(external_ids := well_data.as_external_ids()) > IN_FILTER_LIMIT:
                facility_specification_edges = self.facility_specifications.list(limit=-1)
            else:
                facility_specification_edges = self.facility_specifications.list(external_ids, limit=-1)
            self._set_facility_specifications(well_data, facility_specification_edges)
            if len(external_ids := well_data.as_external_ids()) > IN_FILTER_LIMIT:
                facility_state_edges = self.facility_states.list(limit=-1)
            else:
                facility_state_edges = self.facility_states.list(external_ids, limit=-1)
            self._set_facility_states(well_data, facility_state_edges)
            if len(external_ids := well_data.as_external_ids()) > IN_FILTER_LIMIT:
                geo_context_edges = self.geo_contexts.list(limit=-1)
            else:
                geo_context_edges = self.geo_contexts.list(external_ids, limit=-1)
            self._set_geo_contexts(well_data, geo_context_edges)
            if len(external_ids := well_data.as_external_ids()) > IN_FILTER_LIMIT:
                historical_interest_edges = self.historical_interests.list(limit=-1)
            else:
                historical_interest_edges = self.historical_interests.list(external_ids, limit=-1)
            self._set_historical_interests(well_data, historical_interest_edges)
            if len(external_ids := well_data.as_external_ids()) > IN_FILTER_LIMIT:
                name_alias_edges = self.name_aliases.list(limit=-1)
            else:
                name_alias_edges = self.name_aliases.list(external_ids, limit=-1)
            self._set_name_aliases(well_data, name_alias_edges)
            if len(external_ids := well_data.as_external_ids()) > IN_FILTER_LIMIT:
                technical_assurance_edges = self.technical_assurances.list(limit=-1)
            else:
                technical_assurance_edges = self.technical_assurances.list(external_ids, limit=-1)
            self._set_technical_assurances(well_data, technical_assurance_edges)
            if len(external_ids := well_data.as_external_ids()) > IN_FILTER_LIMIT:
                vertical_measurement_edges = self.vertical_measurements.list(limit=-1)
            else:
                vertical_measurement_edges = self.vertical_measurements.list(external_ids, limit=-1)
            self._set_vertical_measurements(well_data, vertical_measurement_edges)

        return well_data

    @staticmethod
    def _set_facility_events(well_data: Sequence[WellData], facility_event_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in facility_event_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for well_datum in well_data:
            node_id = well_datum.id_tuple()
            if node_id in edges_by_start_node:
                well_datum.facility_events = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_facility_operators(well_data: Sequence[WellData], facility_operator_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in facility_operator_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for well_datum in well_data:
            node_id = well_datum.id_tuple()
            if node_id in edges_by_start_node:
                well_datum.facility_operators = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_facility_specifications(well_data: Sequence[WellData], facility_specification_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in facility_specification_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for well_datum in well_data:
            node_id = well_datum.id_tuple()
            if node_id in edges_by_start_node:
                well_datum.facility_specifications = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_facility_states(well_data: Sequence[WellData], facility_state_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in facility_state_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for well_datum in well_data:
            node_id = well_datum.id_tuple()
            if node_id in edges_by_start_node:
                well_datum.facility_states = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_geo_contexts(well_data: Sequence[WellData], geo_context_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in geo_context_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for well_datum in well_data:
            node_id = well_datum.id_tuple()
            if node_id in edges_by_start_node:
                well_datum.geo_contexts = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_historical_interests(well_data: Sequence[WellData], historical_interest_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in historical_interest_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for well_datum in well_data:
            node_id = well_datum.id_tuple()
            if node_id in edges_by_start_node:
                well_datum.historical_interests = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_name_aliases(well_data: Sequence[WellData], name_alias_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in name_alias_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for well_datum in well_data:
            node_id = well_datum.id_tuple()
            if node_id in edges_by_start_node:
                well_datum.name_aliases = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_technical_assurances(well_data: Sequence[WellData], technical_assurance_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in technical_assurance_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for well_datum in well_data:
            node_id = well_datum.id_tuple()
            if node_id in edges_by_start_node:
                well_datum.technical_assurances = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_vertical_measurements(well_data: Sequence[WellData], vertical_measurement_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in vertical_measurement_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for well_datum in well_data:
            node_id = well_datum.id_tuple()
            if node_id in edges_by_start_node:
                well_datum.vertical_measurements = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


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
    default_vertical_crsid: str | list[str] | None = None,
    default_vertical_crsid_prefix: str | None = None,
    default_vertical_measurement_id: str | list[str] | None = None,
    default_vertical_measurement_id_prefix: str | None = None,
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
    initial_operator_id: str | list[str] | None = None,
    initial_operator_id_prefix: str | None = None,
    interest_type_id: str | list[str] | None = None,
    interest_type_id_prefix: str | None = None,
    operating_environment_id: str | list[str] | None = None,
    operating_environment_id_prefix: str | None = None,
    outcome_id: str | list[str] | None = None,
    outcome_id_prefix: str | None = None,
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
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
    spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    status_summary_id: str | list[str] | None = None,
    status_summary_id_prefix: str | None = None,
    technical_assurance_type_id: str | list[str] | None = None,
    technical_assurance_type_id_prefix: str | None = None,
    version_creation_reason: str | list[str] | None = None,
    version_creation_reason_prefix: str | None = None,
    was_business_interest_financial_non_operated: bool | None = None,
    was_business_interest_financial_operated: bool | None = None,
    was_business_interest_obligatory: bool | None = None,
    was_business_interest_technical: bool | None = None,
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
    if default_vertical_crsid and isinstance(default_vertical_crsid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("DefaultVerticalCRSID"), value=default_vertical_crsid))
    if default_vertical_crsid and isinstance(default_vertical_crsid, list):
        filters.append(dm.filters.In(view_id.as_property_ref("DefaultVerticalCRSID"), values=default_vertical_crsid))
    if default_vertical_crsid_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("DefaultVerticalCRSID"), value=default_vertical_crsid_prefix)
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
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
