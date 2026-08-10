from unittest.mock import MagicMock

import pytest
from cognite.client import ClientConfig
from cognite.client.testing import monkeypatch_cognite_client
from omni import OmniClient


class TestSelectMethod:
    @pytest.mark.filterwarnings(
        r"error:You may be trying to combine two \(or more\) filters using 'and' or 'or'.*:UserWarning"
    )
    def test_dump_yaml(self) -> None:
        with monkeypatch_cognite_client() as client:
            client.config = MagicMock(spec=ClientConfig)
            client.config.client_name = "CognitePygen"
            pygen = OmniClient(client)

        query_yaml = pygen.connection_item_a.select().other_direct._dump_yaml()

        assert isinstance(query_yaml, str)

    @pytest.mark.filterwarnings(
        r"error:You may be trying to combine two \(or more\) filters using 'and' or 'or'.*:UserWarning"
    )
    def test_select_no_user_warning(self) -> None:
        with monkeypatch_cognite_client() as client:
            client.config = MagicMock(spec=ClientConfig)
            client.config.client_name = "CognitePygen"
            pygen = OmniClient(client)
        pygen.connection_item_a.select().name.equals("test").list_full()
