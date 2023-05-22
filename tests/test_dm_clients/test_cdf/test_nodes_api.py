import pytest

from cognite.pygen.dm_clients.cdf.client_dm_v3 import NodesAPI
from cognite.pygen.dm_clients.cdf.data_classes_dm_v3 import Node
from tests.test_dm_clients.test_cdf._utils import *  # noqa


@pytest.fixture
def mock_view(mocker):
    return mocker.Mock(
        space="mockerspace",
        externalId="mockId",
        version="mockver",
    )


@pytest.fixture
def make_api(mocker, make_cognite_client):
    def _make_api(responses):
        return NodesAPI(
            config=mocker.MagicMock(project="mock_proj"),
            api_version="mock_api_version",
            cognite_client=make_cognite_client(responses),
        )

    return _make_api


def test_list_empty(mock_view, make_api):
    mock_api = make_api([{"items": []}])
    value = mock_api.list(mock_view)
    assert value == []


def test_list(mock_view, make_api):
    mock_api = make_api(
        [
            {
                "items": [
                    {
                        "externalId": "mockNode1",
                        "space": "mockerspace",
                        "properties": {
                            "mockerspace": {
                                "mockId": {
                                    "foo": "bar",
                                },
                            },
                        },
                    },
                ],
            },
        ],
    )
    value = mock_api.list(mock_view)
    expected = [
        Node(
            instanceType="node",
            externalId="mockNode1",
            space="mockerspace",
            properties={
                "mockerspace": {
                    "mockId": {
                        "foo": "bar",
                    },
                },
            },
        ),
    ]
    assert value == expected
