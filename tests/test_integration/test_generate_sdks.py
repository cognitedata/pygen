from pathlib import Path

from cognite.client import CogniteClient

from cognite.pygen import generate_sdk
from tests.constants import OMNI_SDK


def test_generate_omni(cognite_client: CogniteClient, tmp_path: Path) -> None:
    generate_sdk(
        OMNI_SDK.data_model_ids,
        cognite_client,
        output_dir=tmp_path,
    )
