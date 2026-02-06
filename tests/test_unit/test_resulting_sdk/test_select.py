import pytest
from cognite.client.testing import monkeypatch_cognite_client
from omni import OmniClient


class TestSelectMethod:
    @pytest.mark.filterwarnings(
        r"error:You may be trying to combine two \(or more\) filters using 'and' or 'or'.*:UserWarning"
    )
    def test_dump_yaml(self) -> None:
        with monkeypatch_cognite_client() as client:
            pygen = OmniClient(client)

        query_yaml = pygen.connection_item_a.select().other_direct._dump_yaml()

        assert isinstance(query_yaml, str)
