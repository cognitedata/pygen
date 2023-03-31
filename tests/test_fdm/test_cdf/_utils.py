import pytest
from cognite.client.testing import CogniteClientMock

__all__ = [
    "make_cognite_client",
]


@pytest.fixture
def make_cognite_client(mocker):
    def _make_cognite_client(responses):
        client = CogniteClientMock()
        client.post.return_value = mocker.Mock(json=mocker.Mock(side_effect=responses))
        client.get.return_value = mocker.Mock(json=mocker.Mock(side_effect=responses))
        return client

    return _make_cognite_client
