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
from . import data_classes


class OSDUClient:
    """
    OSDUClient

    Generated with:
        pygen = 0.30.5
        cognite-sdk = 6.39.3
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
        view_by_write_class = {
            data_classes.AcceptableUsageApply: dm.ViewId(
                "IntegrationTestsImmutable", "AcceptableUsage", "d7e8986cd55d22"
            ),
            data_classes.AclApply: dm.ViewId("IntegrationTestsImmutable", "Acl", "1c4f4a5942a9a8"),
            data_classes.AncestryApply: dm.ViewId("IntegrationTestsImmutable", "Ancestry", "624b46e28cdd69"),
            data_classes.ArtefactsApply: dm.ViewId("IntegrationTestsImmutable", "Artefacts", "7a44a1f4dac367"),
            data_classes.AsIngestedCoordinatesApply: dm.ViewId(
                "IntegrationTestsImmutable", "AsIngestedCoordinates", "da1e4eb90494da"
            ),
            data_classes.AvailableTrajectoryStationPropertiesApply: dm.ViewId(
                "IntegrationTestsImmutable", "AvailableTrajectoryStationProperties", "e1c516b799081a"
            ),
            data_classes.DrillingReasonsApply: dm.ViewId(
                "IntegrationTestsImmutable", "DrillingReasons", "220055a8165644"
            ),
            data_classes.FacilityEventsApply: dm.ViewId(
                "IntegrationTestsImmutable", "FacilityEvents", "1b7526673ad990"
            ),
            data_classes.FacilityOperatorsApply: dm.ViewId(
                "IntegrationTestsImmutable", "FacilityOperators", "935498861713d0"
            ),
            data_classes.FacilitySpecificationsApply: dm.ViewId(
                "IntegrationTestsImmutable", "FacilitySpecifications", "1b7ddbd5d36655"
            ),
            data_classes.FacilityStatesApply: dm.ViewId(
                "IntegrationTestsImmutable", "FacilityStates", "a12316ff3d8033"
            ),
            data_classes.FeaturesApply: dm.ViewId("IntegrationTestsImmutable", "Features", "df91e0a3bad68c"),
            data_classes.GeoContextsApply: dm.ViewId("IntegrationTestsImmutable", "GeoContexts", "cec36d5139aade"),
            data_classes.GeographicBottomHoleLocationApply: dm.ViewId(
                "IntegrationTestsImmutable", "GeographicBottomHoleLocation", "a82995ae29bc5c"
            ),
            data_classes.GeometryApply: dm.ViewId("IntegrationTestsImmutable", "Geometry", "fc702ec6877c79"),
            data_classes.HistoricalInterestsApply: dm.ViewId(
                "IntegrationTestsImmutable", "HistoricalInterests", "7399eff7364ba6"
            ),
            data_classes.LegalApply: dm.ViewId("IntegrationTestsImmutable", "Legal", "508188c6379675"),
            data_classes.LineageAssertionsApply: dm.ViewId(
                "IntegrationTestsImmutable", "LineageAssertions", "ef344f6030d778"
            ),
            data_classes.MetaApply: dm.ViewId("IntegrationTestsImmutable", "Meta", "bf181692a967b6"),
            data_classes.NameAliasesApply: dm.ViewId("IntegrationTestsImmutable", "NameAliases", "b0ef9b17280885"),
            data_classes.ProjectedBottomHoleLocationApply: dm.ViewId(
                "IntegrationTestsImmutable", "ProjectedBottomHoleLocation", "447a307957e5b7"
            ),
            data_classes.ReviewersApply: dm.ViewId("IntegrationTestsImmutable", "Reviewers", "a7b641adc001b9"),
            data_classes.SpatialAreaApply: dm.ViewId("IntegrationTestsImmutable", "SpatialArea", "312323f14f3d3f"),
            data_classes.SpatialLocationApply: dm.ViewId(
                "IntegrationTestsImmutable", "SpatialLocation", "697432f011ef60"
            ),
            data_classes.SpatialPointApply: dm.ViewId("IntegrationTestsImmutable", "SpatialPoint", "7af8800660eef4"),
            data_classes.TagsApply: dm.ViewId("IntegrationTestsImmutable", "Tags", "77ace80e524925"),
            data_classes.TechnicalAssurancesApply: dm.ViewId(
                "IntegrationTestsImmutable", "TechnicalAssurances", "20cfc9c180f3df"
            ),
            data_classes.UnacceptableUsageApply: dm.ViewId(
                "IntegrationTestsImmutable", "UnacceptableUsage", "24f60e09e7bb1b"
            ),
            data_classes.VerticalMeasurementApply: dm.ViewId(
                "IntegrationTestsImmutable", "VerticalMeasurement", "fd63ec6e91292f"
            ),
            data_classes.VerticalMeasurementsApply: dm.ViewId(
                "IntegrationTestsImmutable", "VerticalMeasurements", "d8c3b28a0d0dfb"
            ),
            data_classes.WellApply: dm.ViewId("IntegrationTestsImmutable", "Well", "952d7e55cdf2cc"),
            data_classes.WellboreApply: dm.ViewId("IntegrationTestsImmutable", "Wellbore", "7a44cf38aa4fe7"),
            data_classes.WellboreCostsApply: dm.ViewId("IntegrationTestsImmutable", "WellboreCosts", "b4f71248f398a2"),
            data_classes.WellboreDataApply: dm.ViewId("IntegrationTestsImmutable", "WellboreData", "6349cf734b294e"),
            data_classes.WellboreTrajectoryApply: dm.ViewId(
                "IntegrationTestsImmutable", "WellboreTrajectory", "5c4afa33e6bd65"
            ),
            data_classes.WellboreTrajectoryDataApply: dm.ViewId(
                "IntegrationTestsImmutable", "WellboreTrajectoryData", "d35eace9691587"
            ),
            data_classes.WellDataApply: dm.ViewId("IntegrationTestsImmutable", "WellData", "ed82310421bd56"),
            data_classes.WgsCoordinatesApply: dm.ViewId(
                "IntegrationTestsImmutable", "Wgs84Coordinates", "d6030081373896"
            ),
        }

        self.acceptable_usage = AcceptableUsageAPI(client, view_by_write_class)
        self.acl = AclAPI(client, view_by_write_class)
        self.ancestry = AncestryAPI(client, view_by_write_class)
        self.artefacts = ArtefactsAPI(client, view_by_write_class)
        self.as_ingested_coordinates = AsIngestedCoordinatesAPI(client, view_by_write_class)
        self.available_trajectory_station_properties = AvailableTrajectoryStationPropertiesAPI(
            client, view_by_write_class
        )
        self.drilling_reasons = DrillingReasonsAPI(client, view_by_write_class)
        self.facility_events = FacilityEventsAPI(client, view_by_write_class)
        self.facility_operators = FacilityOperatorsAPI(client, view_by_write_class)
        self.facility_specifications = FacilitySpecificationsAPI(client, view_by_write_class)
        self.facility_states = FacilityStatesAPI(client, view_by_write_class)
        self.features = FeaturesAPI(client, view_by_write_class)
        self.geo_contexts = GeoContextsAPI(client, view_by_write_class)
        self.geographic_bottom_hole_location = GeographicBottomHoleLocationAPI(client, view_by_write_class)
        self.geometry = GeometryAPI(client, view_by_write_class)
        self.historical_interests = HistoricalInterestsAPI(client, view_by_write_class)
        self.legal = LegalAPI(client, view_by_write_class)
        self.lineage_assertions = LineageAssertionsAPI(client, view_by_write_class)
        self.meta = MetaAPI(client, view_by_write_class)
        self.name_aliases = NameAliasesAPI(client, view_by_write_class)
        self.projected_bottom_hole_location = ProjectedBottomHoleLocationAPI(client, view_by_write_class)
        self.reviewers = ReviewersAPI(client, view_by_write_class)
        self.spatial_area = SpatialAreaAPI(client, view_by_write_class)
        self.spatial_location = SpatialLocationAPI(client, view_by_write_class)
        self.spatial_point = SpatialPointAPI(client, view_by_write_class)
        self.tags = TagsAPI(client, view_by_write_class)
        self.technical_assurances = TechnicalAssurancesAPI(client, view_by_write_class)
        self.unacceptable_usage = UnacceptableUsageAPI(client, view_by_write_class)
        self.vertical_measurement = VerticalMeasurementAPI(client, view_by_write_class)
        self.vertical_measurements = VerticalMeasurementsAPI(client, view_by_write_class)
        self.well = WellAPI(client, view_by_write_class)
        self.wellbore = WellboreAPI(client, view_by_write_class)
        self.wellbore_costs = WellboreCostsAPI(client, view_by_write_class)
        self.wellbore_data = WellboreDataAPI(client, view_by_write_class)
        self.wellbore_trajectory = WellboreTrajectoryAPI(client, view_by_write_class)
        self.wellbore_trajectory_data = WellboreTrajectoryDataAPI(client, view_by_write_class)
        self.well_data = WellDataAPI(client, view_by_write_class)
        self.wgs_84_coordinates = WgsCoordinatesAPI(client, view_by_write_class)

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
