from wind_turbine import WindTurbineClient
from wind_turbine import data_classes as data_cls


def test_edges_to_pandas(turbine_client: WindTurbineClient) -> None:
    df = turbine_client.wind_turbine.metmast_edge.list(max_distance=1000, limit=3).to_pandas()
    assert len(df) <= 3
    assert not df.empty
    assert "distance" in df.columns
    assert sum(df["distance"] <= 1000) == len(df)


def test_upsert_with_edge(turbine_client: WindTurbineClient) -> None:
    new_turbine = data_cls.WindTurbineWrite(
        external_id="doctriono_b",
        name="A new Wind Turbine",
        capacity=8.0,
        metmast=[
            data_cls.DistanceWrite(
                distance=500.0,
                end_node=data_cls.MetmastWrite(
                    external_id="doctrino_weather",
                    position=42.0,
                ),
            )
        ],
    )

    try:
        created = turbine_client.upsert(new_turbine)

        assert len(created.nodes) == 2
        assert len(created.edges) == 1
    finally:
        turbine_client.delete(new_turbine)
