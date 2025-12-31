import pytest

from cognite.pygen._client.models import DataModelResponseWithViews
from cognite.pygen._generator.transformer import to_pygen_model
from cognite.pygen._pygen_model import PygenSDKModel


class TestToPygenModel:
    @pytest.mark.xfail(raises=NotImplementedError, strict=True)
    def test_example_data_model(self, example_data_model_response: DataModelResponseWithViews):
        pygen_model = to_pygen_model(example_data_model_response, "python")
        assert isinstance(pygen_model, PygenSDKModel)
