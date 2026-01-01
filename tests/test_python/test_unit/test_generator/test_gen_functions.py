from functools import lru_cache
from pathlib import Path

import pytest

from cognite.pygen._example_datamodel import EXTERNAL_ID, SPACE, VERSION
from cognite.pygen._generator.config import PygenSDKConfig
from cognite.pygen._generator.gen_functions import generate_sdk
from cognite.pygen._python.instance_api.config import PygenClientConfig
from tests.test_python.constants import EXAMPLES
from tests.test_python.test_unit.conftest import MockCredentials
from tests.test_python.test_unit.test_generator.conftest import create_example_data_model_response
from tests.test_python.utils import monkeypatch_pygen_client

SDK_NAME_PYTHON = "example_sdk_python"


@lru_cache
def load_example_model(example_name: str) -> dict[Path, str]:
    path = EXAMPLES / example_name
    if not path.exists():
        raise FileNotFoundError(f"Example data model '{example_name}' not found at path: {path}")
    sdk: dict[Path, str] = {}
    for file in path.rglob("**/*"):
        if file.is_file():
            relative_path = file.relative_to(path)
            sdk[relative_path] = file.read_text(encoding="utf-8")
    return sdk


@pytest.fixture(scope="session")
def actual_python_sdk_example_model() -> dict[Path, str]:
    with monkeypatch_pygen_client() as mocked_client:
        mocked_client.data_models.retrieve.return_value = [create_example_data_model_response()]
        sdk_config = PygenSDKConfig(
            top_level_package=SDK_NAME_PYTHON,
            client_name="ExamplePygenClient",
        )
        client_config = PygenClientConfig(
            cdf_url="https://example.cognitedata.com",
            project="pygen",
            credentials=MockCredentials(),
        )
        sdk_files = generate_sdk(
            space=SPACE,
            external_id=EXTERNAL_ID,
            version=VERSION,
            sdk_config=sdk_config,
            client_config=client_config,
            output_format="python",
        )
    return sdk_files


@pytest.mark.parametrize("file_path", load_example_model(SDK_NAME_PYTHON).keys(), ids=lambda p: p.as_posix())
def test_generate_python_sdk_example_model(file_path: Path, actual_python_sdk_example_model: dict[Path, str]) -> None:
    expected_filepath = EXAMPLES / file_path
    if not expected_filepath.exists():
        pytest.fail(f"Expected file {file_path} not found in examples directory.")
    expected_content = expected_filepath.read_text(encoding="utf-8")

    actual_content = actual_python_sdk_example_model.get(file_path)
    if actual_content is None:
        pytest.fail(f"Generated SDK is missing expected file: {file_path}")

    assert actual_content == expected_content, f"Content mismatch for file: {file_path}"
