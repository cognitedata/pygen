from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

import pytest

from cognite.pygen._generation.python.instance_api._utils import (
    MAX_TIMESTAMP_MS,
    MIN_TIMESTAMP_MS,
    datetime_to_ms,
    ms_to_datetime,
)


def datetime_ms_test_cases() -> Iterable[tuple]:
    yield pytest.param(
        datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        1704067200000,
        id="2024-01-01T00:00:00Z to ms",
    )
    yield pytest.param(
        datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        0,
        id="1970-01-01T00:00:00Z to ms",
    )
    yield pytest.param(
        datetime(1954, 7, 20, 12, 34, 56, tzinfo=timezone.utc),
        -487596304000,
        id="1954-07-20T12:34:56Z to ms (negative)",
    )


class TestDatetimeUtils:
    @pytest.mark.parametrize("dt,ms", datetime_ms_test_cases())
    def test_datetime_to_ms(self, dt: datetime, ms: int) -> None:
        assert datetime_to_ms(dt) == ms

    @pytest.mark.parametrize("dt,ms", datetime_ms_test_cases())
    def test_ms_to_datetime(self, dt: datetime, ms: int) -> None:
        assert ms_to_datetime(ms) == dt

    @pytest.mark.parametrize("dt,ms", datetime_ms_test_cases())
    def test_datetime_ms_roundtrip(self, dt: datetime, ms: int) -> None:
        assert ms_to_datetime(datetime_to_ms(dt)) == dt
        assert datetime_to_ms(ms_to_datetime(ms)) == ms

    @pytest.mark.parametrize(
        "ms",
        [
            pytest.param(MAX_TIMESTAMP_MS + 1, id="Above max timestamp ms"),
            pytest.param(MIN_TIMESTAMP_MS - 1, id="Below min timestamp ms"),
        ],
    )
    def test_ms_to_datetime_out_of_bounds(self, ms: int) -> None:
        with pytest.raises(ValueError):
            ms_to_datetime(ms)

    @pytest.mark.parametrize(
        "dt",
        [
            pytest.param(
                datetime(
                    2025,
                    1,
                    1,
                    0,
                    0,
                    0,
                ),
                id="Missing tzinfo",
            ),
            pytest.param(
                datetime(1984, 12, 31, 23, 59, 59, tzinfo=timezone(timedelta(hours=1))), id="Oslo tzinfo (non-UTC)"
            ),
        ],
    )
    def test_datetime_to_ms_out_of_bounds(self, dt: datetime) -> None:
        with pytest.raises(ValueError):
            datetime_to_ms(dt)
