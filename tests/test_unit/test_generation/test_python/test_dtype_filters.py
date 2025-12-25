from datetime import date, datetime

import pytest

from cognite.pygen._generation.python.example._data_class import PrimitiveNullable, PrimitiveNullableFilter
from cognite.pygen._generation.python.instance_api.models.filters import FilterAdapter


@pytest.fixture
def primitive_filter() -> PrimitiveNullableFilter:
    return PrimitiveNullableFilter(operator="and")


def _get_property_path(property: str) -> list[str]:
    view_id = PrimitiveNullable._view_id
    return [view_id.space, f"{view_id.external_id}/{view_id.version}", property]


class TestDataTypeFilters:
    def test_boolean_filter(self, primitive_filter: PrimitiveNullableFilter) -> None:
        boolean_filter = primitive_filter.boolean
        boolean_filter.equals(True)
        assert boolean_filter.dump() == {
            "equals": {"property": _get_property_path("boolean"), "value": True}
        }

    def test_float_filter_equals(self, primitive_filter: PrimitiveNullableFilter) -> None:
        float_filter = primitive_filter.float32
        float_filter.equals(3.14)
        assert FilterAdapter.dump_python(float_filter.as_filter()) == {
            "equals": {"property": _get_property_path("float32"), "value": 3.14}
        }

    def test_float_filter_range(self, primitive_filter: PrimitiveNullableFilter) -> None:
        float_filter = primitive_filter.float64
        float_filter.greater_than(1.0).less_than(10.0)
        assert FilterAdapter.dump_python(float_filter.as_filter(), exclude_none=True) == {
            "range": {"property": _get_property_path("float64"), "gt": 1.0, "lt": 10.0}
        }

    def test_float_filter_range_ge_le(self, primitive_filter: PrimitiveNullableFilter) -> None:
        float_filter = primitive_filter.float64
        float_filter.greater_than_or_equals(2.5).less_than_or_equals(7.5)
        assert FilterAdapter.dump_python(float_filter.as_filter(), exclude_none=True) == {
            "range": {"property": _get_property_path("float64"), "gte": 2.5, "lte": 7.5}
        }

    def test_integer_filter_equals(self, primitive_filter: PrimitiveNullableFilter) -> None:
        int_filter = primitive_filter.int32
        int_filter.equals(42)
        assert FilterAdapter.dump_python(int_filter.as_filter()) == {
            "equals": {"property": _get_property_path("int32"), "value": 42}
        }

    def test_integer_filter_range(self, primitive_filter: PrimitiveNullableFilter) -> None:
        int_filter = primitive_filter.int64
        int_filter.greater_than_or_equals(0).less_than_or_equals(100)
        assert FilterAdapter.dump_python(int_filter.as_filter(), exclude_none=True) == {
            "range": {"property": _get_property_path("int64"), "gte": 0, "lte": 100}
        }

    def test_integer_filter_range_ge_lt(self, primitive_filter: PrimitiveNullableFilter) -> None:
        int_filter = primitive_filter.int64
        int_filter.greater_than(10).less_than(50)
        assert FilterAdapter.dump_python(int_filter.as_filter(), exclude_none=True) == {
            "range": {"property": _get_property_path("int64"), "gt": 10, "lt": 50}
        }

    def test_datetime_filter_equals(self, primitive_filter: PrimitiveNullableFilter) -> None:
        dt = datetime(2024, 1, 15, 12, 30, 0)
        datetime_filter = primitive_filter.timestamp
        datetime_filter.equals(dt)
        assert FilterAdapter.dump_python(datetime_filter.as_filter()) == {
            "equals": {"property": _get_property_path("timestamp"), "value": dt.isoformat(timespec="milliseconds")}
        }

    def test_datetime_filter_range(self, primitive_filter: PrimitiveNullableFilter) -> None:
        start = datetime(2024, 1, 1)
        end = datetime(2024, 12, 31)
        datetime_filter = primitive_filter.timestamp
        datetime_filter.greater_than_or_equals(start).less_than(end)
        assert FilterAdapter.dump_python(datetime_filter.as_filter(), exclude_none=True) == {
            "range": {
                "property": _get_property_path("timestamp"),
                "gte": start.isoformat(timespec="milliseconds"),
                "lt": end.isoformat(timespec="milliseconds"),
            }
        }

    def test_datetime_filter_range_with_str(self, primitive_filter: PrimitiveNullableFilter) -> None:
        start = "2024-01-01T00:00:00.000Z"
        end = "2024-12-31T23:59:59.999Z"
        datetime_filter = primitive_filter.timestamp
        datetime_filter.greater_than(start).less_than_or_equals(end)
        assert FilterAdapter.dump_python(datetime_filter.as_filter(), exclude_none=True) == {
            "range": {
                "property": _get_property_path("timestamp"),
                "gt": start,
                "lte": end,
            }
        }

    def test_date_filter_equals(self, primitive_filter: PrimitiveNullableFilter) -> None:
        d = date(2024, 1, 15)
        date_filter = primitive_filter.date_
        date_filter.equals(d)
        assert FilterAdapter.dump_python(date_filter.as_filter()) == {
            "equals": {"property": _get_property_path("date"), "value": d.isoformat()}
        }

    def test_date_filter_range(self, primitive_filter: PrimitiveNullableFilter) -> None:
        start = date(2024, 1, 1)
        end = date(2024, 12, 31)
        date_filter = primitive_filter.date_
        date_filter.greater_than_or_equals(start).less_than_or_equals(end)
        assert FilterAdapter.dump_python(date_filter.as_filter(), exclude_none=True) == {
            "range": {"property": _get_property_path("date"), "lte": end.isoformat(), "gte": start.isoformat()}
        }

    def test_date_filter_range_with_str(self, primitive_filter: PrimitiveNullableFilter) -> None:
        start = "2024-01-01"
        end = "2024-12-31"
        date_filter = primitive_filter.date_
        date_filter.greater_than(start).less_than(end)
        assert FilterAdapter.dump_python(date_filter.as_filter(), exclude_none=True) == {
            "range": {"property": _get_property_path("date"), "gt": start, "lt": end}
        }

    def test_text_filter_equals(self, primitive_filter: PrimitiveNullableFilter) -> None:
        text_filter = primitive_filter.text
        text_filter.equals("hello")
        assert FilterAdapter.dump_python(text_filter.as_filter()) == {
            "equals": {"property": _get_property_path("text"), "value": "hello"}
        }

    def test_text_filter_prefix(self, primitive_filter: PrimitiveNullableFilter) -> None:
        text_filter = primitive_filter.text
        text_filter.prefix("test")
        assert FilterAdapter.dump_python(text_filter.as_filter()) == {
            "prefix": {"property": _get_property_path("text"), "value": "test"}
        }

    def test_text_filter_in(self, primitive_filter: PrimitiveNullableFilter) -> None:
        text_filter = primitive_filter.text
        text_filter.in_(["value1", "value2", "value3"])
        assert FilterAdapter.dump_python(text_filter.as_filter()) == {
            "in": {"property": _get_property_path("text"), "values": ["value1", "value2", "value3"]}
        }

    def test_text_filter_multiple_conditions(self, primitive_filter: PrimitiveNullableFilter) -> None:
        text_filter = primitive_filter.text
        text_filter.prefix("start").equals("exact")
        assert FilterAdapter.dump_python(text_filter.as_filter()) == {
            "and": [
                {"prefix": {"property": _get_property_path("text"), "value": "start"}},
                {"equals": {"property": _get_property_path("text"), "value": "exact"}},
            ]
        }


class TestFilterContainer:
    def test_empty_filter_container(self) -> None:
        assert PrimitiveNullableFilter(operator="and").as_filter() is None

    def test_single_filter(self, primitive_filter: PrimitiveNullableFilter) -> None:
        primitive_filter.int32.equals(5)
        single_filter = primitive_filter.as_filter()
        assert FilterAdapter.dump_python(single_filter) == {
            "equals": {"property": _get_property_path("int32"), "value": 5}
        }

    def test_combined_filters(self, primitive_filter: PrimitiveNullableFilter) -> None:
        primitive_filter.text.equals("example")
        primitive_filter.int32.greater_than(10)
        combined_filter = primitive_filter.as_filter()
        assert FilterAdapter.dump_python(combined_filter, exclude_none=True) == {
            "and": [
                {"equals": {"property": _get_property_path("text"), "value": "example"}},
                {"range": {"property": _get_property_path("int32"), "gt": 10}},
            ]
        }
