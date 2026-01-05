from pathlib import Path

from wind_turbine import WindTurbineClient


def test_download_files(turbine_client: WindTurbineClient, tmp_path: Path) -> None:
    (turbine_client.wind_turbine.select().name.equals("hornsea_1_mill_1").datasheets.content.download(tmp_path))

    assert len(list(tmp_path.iterdir())) > 0
