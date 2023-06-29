from datetime import datetime

from shop.client import ShopClient
from shop.client.data_classes import CaseApply, CommandConfigApply


def test_case_list(shop_client: ShopClient):
    cases = shop_client.cases.list(limit=-1)

    assert len(cases) > 1, "There should be more than one case in the cdf"


def test_case_retrieve(shop_client: ShopClient):
    kelly = shop_client.cases.retrieve("shop:case:4:Kelly")

    assert kelly.external_id == "shop:case:4:Kelly"
    assert len(kelly.cut_files) == 8
    assert kelly.arguments == "Hunter LLC"


def test_case_retrieve_multiple(shop_client: ShopClient):
    cases = shop_client.cases.retrieve(["shop:case:4:Kelly", "shop:case:3:Frederick"])

    assert sorted([c.external_id for c in cases]) == sorted(["shop:case:4:Kelly", "shop:case:3:Frederick"])
    assert len(cases) == 2


def test_case_apply_and_delete(shop_client: ShopClient):
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
    result = None
    try:
        result = shop_client.cases.apply(case, replace=True)

        assert len(result.nodes) == 2
        assert len(result.edges) == 0
    finally:
        if result is not None:
            shop_client.cases.delete([n.as_id() for n in result.nodes])
