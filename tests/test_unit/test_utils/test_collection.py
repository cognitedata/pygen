from collections import UserList
from collections.abc import Collection
from typing import Any

import pytest

from cognite.pygen._utils.collection import chunker_sequence, humanize_collection


class MyList(UserList): ...


class TestChunkerSequence:
    def test_chunker_sequence(self) -> None:
        data = MyList([1, 2, 3, 4, 5, 6, 7, 8, 9])
        chunks = list(chunker_sequence(data, size=4))

        for chunk in chunks:
            assert isinstance(chunk, MyList)
        # MyList([1, 2, 3, 4]) == [1, 2, 3, 4] evaluates to True
        assert chunks == [
            [1, 2, 3, 4],
            [5, 6, 7, 8],
            [9],
        ]

    def test_chunker_sequence_empty(self) -> None:
        assert list(chunker_sequence([], size=3)) == []


class TestHumanizeCollection:
    @pytest.mark.parametrize(
        "input_collection, args, expected_output",
        [
            ([], {}, ""),
            (["A"], {}, "A"),
            (["A", "B"], {}, "A and B"),
            (["A", "B", "C"], {"bind_word": "or"}, "A, B or C"),
            (["D", "A", "C", "D"], {"sort": False}, "D, A, C and D"),
        ],
    )
    def test_humanize_collection(
        self, input_collection: Collection[Any], args: dict[str, Any], expected_output: str
    ) -> None:
        assert humanize_collection(input_collection, **args) == expected_output
