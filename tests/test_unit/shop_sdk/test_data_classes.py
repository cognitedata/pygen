from datetime import datetime

from shop.client.data_classes import CaseApply, CommandConfigApply


def test_to_instances_apply_case():
    # Arrange
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    case = CaseApply(
        external_id="shop:case:integration_test",
        name="Integration test",
        scenario="Integration test",
        start_time=datetime.strptime("2021-01-01T00:00:00Z", date_format),
        end_time=datetime.strptime("2021-01-01T00:00:00Z", date_format),
        command=CommandConfigApply(external_id="shop:command_config:integration_test", configs=["BlueViolet", "Red"]),
        cut_files=["shop:cut_file:1"],
        bid="shop:bid_matrix:8",
        bid_histories=["shop:bid_matrix:9"],
        run_status="Running",
        argument="Integration test",
    )

    # Act
    instances = case.to_instances_apply()

    # Assert
    assert len(instances.nodes) == 2
    assert len(instances.edges) == 0
