from datetime import datetime

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from shop.client.data_classes import CaseApply, CommandConfigApply
else:
    # is pydantic v1
    from shop_pydantic_v1.client.data_classes import CaseApply, CommandConfigApply


def test_to_instances_apply_case():
    # Arrange
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    case = CaseApply(
        external_id="shop:case:integration_test",
        name="Integration test",
        scenario="Integration test",
        start_time=datetime.strptime("2021-01-01T00:00:00Z", date_format),
        end_time=datetime.strptime("2021-01-01T00:00:00Z", date_format),
        commands=CommandConfigApply(external_id="shop:command_config:integration_test", configs=["BlueViolet", "Red"]),
        cut_files=["shop:cut_file:1"],
        bid="shop:bid_matrix:8",
        bid_history=["shop:bid_matrix:9"],
        run_status="Running",
        arguments="Integration test",
    )

    # Act
    instances = case.to_instances_apply()

    # Assert
    assert len(instances.nodes) == 2
    assert len(instances.edges) == 0
