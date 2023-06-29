from datetime import datetime

from shop.client import ShopClient
from shop.client.data_classes import Case, CaseApply, CaseList, CommandConfigApply


def test_case_list(shop_client: ShopClient):
    cases = shop_client.cases.list(limit=-1)

    assert isinstance(cases, CaseList)


def test_case_retrieve(shop_client: ShopClient):
    kelly = shop_client.cases.retrieve("shop:case:4:Kelly")

    assert isinstance(kelly, Case)
    assert len(kelly.cut_files) == 8
    assert kelly.arguments == "Hunter LLC"


def test_case_retrieve_multiple(shop_client: ShopClient):
    cases = shop_client.cases.retrieve(["shop:case:4:Kelly", "shop:case:3:Frederick"])

    assert isinstance(cases, CaseList)
    assert len(cases) == 2


def test_case_apply_and_delete(shop_client: ShopClient):
    # Arrange
    case = CaseApply(
        external_id="shop:case:integration_test",
        name="Integration test",
        scenario="Integration test",
        start_time=datetime.fromisoformat("2021-01-01T00:00:00Z"),
        end_time=datetime.fromisoformat("2021-01-01T00:00:00Z"),
        commands=CommandConfigApply(external_id="shop:command_config:integration_test", configs=["BlueViolet", "Red"]),
        cut_files=["shop:cut_file:1"],
        bid="shop:bid_matrix:8",
        bid_history=["shop:bid_matrix:9"],
        run_status="Running",
        arguments="Integration test",
    )

    # Act
    try:
        result = shop_client.cases.apply(case, replace=True)

        assert len(result.nodes) == 2
        assert len(result.edges) == 0
    finally:
        shop_client.cases.delete([n.as_id() for n in result.nodes])
