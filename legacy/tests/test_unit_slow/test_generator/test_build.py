import os
from pathlib import Path

import pytest

from cognite.pygen._build import build_wheel
from tests.constants import OMNI_SDK


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="The build package use a subprocess to run the command, which is not supported in GitHub Actions.",
)
def test_build_wheel() -> None:
    data_model = OMNI_SDK.load_data_model()
    local_dir = Path(__file__).parent / "dist"
    build_wheel(data_model, top_level_package="omni_sdk", output_dir=local_dir)
    expected_file = local_dir / f"omni_sdk-{data_model.version}-py3-none-any.whl"
    assert expected_file.exists()

    # Clean up
    expected_file.unlink()
