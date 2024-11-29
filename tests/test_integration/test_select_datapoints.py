from omni import OmniClient
from wind_turbine import WindTurbineClient


def test_retrieve_dataframe(turbine_client: WindTurbineClient) -> None:
    df = (
        turbine_client.wind_turbine.select()
        .name.equals("hornsea_1_mill_1")
        .rotor.rotor_speed_controller.data.retrieve_dataframe(
            limit=10,
            timeseries_limit=100,
        )
    )

    assert not df.empty
    assert len(df) == 10


def test_retrieve_dataframe_classic(omni_client: OmniClient) -> None:
    df = omni_client.cdf_external_references.select().timeseries.data.retrieve_dataframe(
        limit=10,
        timeseries_limit=100,
    )

    assert not df.empty
    assert len(df) == 10
