from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.acceptable_usage import AcceptableUsageAPI
from ._api.acl import AclAPI
from ._api.ancestry import AncestryAPI
from ._api.artefacts import ArtefactsAPI
from ._api.as_ingested_coordinates import AsIngestedCoordinatesAPI
from ._api.available_trajectory_station_properties import AvailableTrajectoryStationPropertiesAPI
from ._api.drilling_reasons import DrillingReasonsAPI
from ._api.facility_events import FacilityEventsAPI
from ._api.facility_operators import FacilityOperatorsAPI
from ._api.facility_specifications import FacilitySpecificationsAPI
from ._api.facility_states import FacilityStatesAPI
from ._api.features import FeaturesAPI
from ._api.geo_contexts import GeoContextsAPI
from ._api.geographic_bottom_hole_location import GeographicBottomHoleLocationAPI
from ._api.geometry import GeometryAPI
from ._api.historical_interests import HistoricalInterestsAPI
from ._api.legal import LegalAPI
from ._api.lineage_assertions import LineageAssertionsAPI
from ._api.meta import MetaAPI
from ._api.name_aliases import NameAliasesAPI
from ._api.projected_bottom_hole_location import ProjectedBottomHoleLocationAPI
from ._api.reviewers import ReviewersAPI
from ._api.spatial_area import SpatialAreaAPI
from ._api.spatial_location import SpatialLocationAPI
from ._api.spatial_point import SpatialPointAPI
from ._api.tags import TagsAPI
from ._api.technical_assurances import TechnicalAssurancesAPI
from ._api.unacceptable_usage import UnacceptableUsageAPI
from ._api.vertical_measurement import VerticalMeasurementAPI
from ._api.vertical_measurements import VerticalMeasurementsAPI
from ._api.well import WellAPI
from ._api.wellbore import WellboreAPI
from ._api.wellbore_costs import WellboreCostsAPI
from ._api.wellbore_data import WellboreDataAPI
from ._api.wellbore_trajectory import WellboreTrajectoryAPI
from ._api.wellbore_trajectory_data import WellboreTrajectoryDataAPI
from ._api.well_data import WellDataAPI
from ._api.wgs_84_coordinates import WgsCoordinatesAPI


class OSDUClient:
    """
    OSDUClient

    Generated with:
        pygen = 0.27.3
        cognite-sdk = 6.37.0
        pydantic = 2.4.2

    Data Model:
        space: IntegrationTestsImmutable
        externalId: OSDUWells
        version: 1
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        self.acceptable_usage = AcceptableUsageAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "AcceptableUsage", "d7e8986cd55d22")
        )
        self.acl = AclAPI(client, dm.ViewId("IntegrationTestsImmutable", "Acl", "1c4f4a5942a9a8"))
        self.ancestry = AncestryAPI(client, dm.ViewId("IntegrationTestsImmutable", "Ancestry", "624b46e28cdd69"))
        self.artefacts = ArtefactsAPI(client, dm.ViewId("IntegrationTestsImmutable", "Artefacts", "7a44a1f4dac367"))
        self.as_ingested_coordinates = AsIngestedCoordinatesAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "AsIngestedCoordinates", "da1e4eb90494da")
        )
        self.available_trajectory_station_properties = AvailableTrajectoryStationPropertiesAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "AvailableTrajectoryStationProperties", "e1c516b799081a")
        )
        self.drilling_reasons = DrillingReasonsAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "DrillingReasons", "220055a8165644")
        )
        self.facility_events = FacilityEventsAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "FacilityEvents", "1b7526673ad990")
        )
        self.facility_operators = FacilityOperatorsAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "FacilityOperators", "935498861713d0")
        )
        self.facility_specifications = FacilitySpecificationsAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "FacilitySpecifications", "1b7ddbd5d36655")
        )
        self.facility_states = FacilityStatesAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "FacilityStates", "a12316ff3d8033")
        )
        self.features = FeaturesAPI(client, dm.ViewId("IntegrationTestsImmutable", "Features", "df91e0a3bad68c"))
        self.geo_contexts = GeoContextsAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "GeoContexts", "cec36d5139aade")
        )
        self.geographic_bottom_hole_location = GeographicBottomHoleLocationAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "GeographicBottomHoleLocation", "a82995ae29bc5c")
        )
        self.geometry = GeometryAPI(client, dm.ViewId("IntegrationTestsImmutable", "Geometry", "fc702ec6877c79"))
        self.historical_interests = HistoricalInterestsAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "HistoricalInterests", "7399eff7364ba6")
        )
        self.legal = LegalAPI(client, dm.ViewId("IntegrationTestsImmutable", "Legal", "508188c6379675"))
        self.lineage_assertions = LineageAssertionsAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "LineageAssertions", "ef344f6030d778")
        )
        self.meta = MetaAPI(client, dm.ViewId("IntegrationTestsImmutable", "Meta", "bf181692a967b6"))
        self.name_aliases = NameAliasesAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "NameAliases", "b0ef9b17280885")
        )
        self.projected_bottom_hole_location = ProjectedBottomHoleLocationAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "ProjectedBottomHoleLocation", "447a307957e5b7")
        )
        self.reviewers = ReviewersAPI(client, dm.ViewId("IntegrationTestsImmutable", "Reviewers", "a7b641adc001b9"))
        self.spatial_area = SpatialAreaAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "SpatialArea", "312323f14f3d3f")
        )
        self.spatial_location = SpatialLocationAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "SpatialLocation", "697432f011ef60")
        )
        self.spatial_point = SpatialPointAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "SpatialPoint", "7af8800660eef4")
        )
        self.tags = TagsAPI(client, dm.ViewId("IntegrationTestsImmutable", "Tags", "77ace80e524925"))
        self.technical_assurances = TechnicalAssurancesAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "TechnicalAssurances", "20cfc9c180f3df")
        )
        self.unacceptable_usage = UnacceptableUsageAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "UnacceptableUsage", "24f60e09e7bb1b")
        )
        self.vertical_measurement = VerticalMeasurementAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "VerticalMeasurement", "fd63ec6e91292f")
        )
        self.vertical_measurements = VerticalMeasurementsAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "VerticalMeasurements", "d8c3b28a0d0dfb")
        )
        self.well = WellAPI(client, dm.ViewId("IntegrationTestsImmutable", "Well", "952d7e55cdf2cc"))
        self.wellbore = WellboreAPI(client, dm.ViewId("IntegrationTestsImmutable", "Wellbore", "7a44cf38aa4fe7"))
        self.wellbore_costs = WellboreCostsAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "WellboreCosts", "b4f71248f398a2")
        )
        self.wellbore_data = WellboreDataAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "WellboreData", "6349cf734b294e")
        )
        self.wellbore_trajectory = WellboreTrajectoryAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "WellboreTrajectory", "5c4afa33e6bd65")
        )
        self.wellbore_trajectory_data = WellboreTrajectoryDataAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "WellboreTrajectoryData", "d35eace9691587")
        )
        self.well_data = WellDataAPI(client, dm.ViewId("IntegrationTestsImmutable", "WellData", "ed82310421bd56"))
        self.wgs_84_coordinates = WgsCoordinatesAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "Wgs84Coordinates", "d6030081373896")
        )

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> OSDUClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> OSDUClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
