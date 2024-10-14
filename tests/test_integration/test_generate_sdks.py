from pathlib import Path

from cognite.client import CogniteClient

from cognite.pygen import generate_sdk, generate_sdk_notebook
from tests.constants import OMNI_SDK


def test_generate_omni(cognite_client: CogniteClient, tmp_path: Path) -> None:
    generate_sdk(
        OMNI_SDK.data_model_ids,
        cognite_client,
        output_dir=tmp_path,
    )


def test_generate_process_industries(cognite_client: CogniteClient, tmp_path: Path) -> None:
    client = generate_sdk_notebook(
        ("cdf_idm", "CogniteProcessIndustries", "v1"),
        cognite_client,
    )

    df = client.cognite_time_series.list(limit=1).to_pandas()

    assert {"space", "externalId", "name", "isString", "isStep"} <= set(df.columns)
