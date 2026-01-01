import pytest

from cognite.pygen._client.models import DataModelResponseWithViews
from cognite.pygen._generator._types import OutputFormat
from cognite.pygen._generator.transformer import to_pygen_model
from cognite.pygen._pygen_model import PygenSDKModel


class TestToPygenModel:
    @pytest.mark.parametrize("output_format", ["python", "typescript"])
    def test_example_data_model(
        self, example_data_model_response: DataModelResponseWithViews, output_format: OutputFormat
    ) -> None:
        pygen_model = to_pygen_model(example_data_model_response, output_format)
        assert isinstance(pygen_model, PygenSDKModel)
