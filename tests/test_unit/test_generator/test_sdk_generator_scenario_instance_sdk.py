import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import APIGenerator, MultiAPIGenerator, SDKGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import SCENARIO_INSTANCE_SDK, ScenarioInstanceFiles


@pytest.fixture
def sdk_generator(scenario_instance_model: dm.DataModel[dm.View]) -> SDKGenerator:
    return SDKGenerator(
        SCENARIO_INSTANCE_SDK.top_level_package, SCENARIO_INSTANCE_SDK.client_name, scenario_instance_model
    )


@pytest.fixture
def multi_api_generator(scenario_instance_model: dm.DataModel[dm.View]) -> MultiAPIGenerator:
    return MultiAPIGenerator(
        SCENARIO_INSTANCE_SDK.top_level_package,
        SCENARIO_INSTANCE_SDK.client_name,
        scenario_instance_model.views,
        scenario_instance_model.space,
    )


@pytest.fixture
def scenario_instance_api_generator(
    multi_api_generator: MultiAPIGenerator, scenario_instance_view: dm.View
) -> APIGenerator:
    api_generator = multi_api_generator[scenario_instance_view.as_id()]
    assert api_generator is not None, "Could not find API generator for scenario instance view"
    return api_generator


def test_generate_api_file_scenario_instance(
    scenario_instance_api_generator: APIGenerator, code_formatter: CodeFormatter
) -> None:
    # Arrange
    expected = ScenarioInstanceFiles.scenario_instance_api.read_text()
    assert scenario_instance_api_generator.data_class.has_primitive_field_of_type(dm.TimeSeriesReference)

    # Act
    actual = scenario_instance_api_generator.generate_api_file(
        SCENARIO_INSTANCE_SDK.top_level_package, SCENARIO_INSTANCE_SDK.client_name
    )
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected
