from cognite.pygen._query.processing import QueryUnpacker


class TestQueryUnpacker:
    def test_empty_response(self) -> None:
        unpacker = QueryUnpacker(steps=[])

        result = unpacker.unpack()

        assert result == [], "Expected empty list for empty response"
