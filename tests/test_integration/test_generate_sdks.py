from pathlib import Path

from cognite.client import CogniteClient

from cognite.pygen import generate_sdk, generate_sdk_notebook
from tests.constants import OMNI_SDK


def test_generate_omni(cognite_client: CogniteClient, tmp_path: Path) -> None:
    generate_sdk(
        OMNI_SDK.data_model_ids,
        cognite_client,
        top_level_package=OMNI_SDK.top_level_package,
        output_dir=tmp_path / Path(OMNI_SDK.top_level_package.replace(".", "/")),
    )


def test_generate_process_industries(cognite_client: CogniteClient, tmp_path: Path) -> None:
    client = generate_sdk_notebook(
        ("cdf_idm", "CogniteProcessIndustries", "v1"),
        cognite_client,
    )

    df = client.cognite_time_series.list(limit=1).to_pandas()

    assert {"space", "external_id", "name", "type_", "is_step"} <= set(df.columns)
