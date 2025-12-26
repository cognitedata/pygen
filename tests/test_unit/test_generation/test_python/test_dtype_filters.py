from datetime import date, datetime
from typing import Literal

import pytest

from cognite.pygen._generation.python.instance_api.models._references import ViewReference
from cognite.pygen._generation.python.instance_api.models.dtype_filters import (
    BooleanFilter,
    DateFilter,
    DateTimeFilter,
    FilterContainer,
    FloatFilter,
    IntegerFilter,
    TextFilter,
)
from cognite.pygen._generation.python.instance_api.models.filters import FilterAdapter

VIEW_ID = ViewReference(space="example_space", external_id="ExampleView", version="v1")


class ExampleFilter(FilterContainer):
    def __init__(self, operator: Literal["and", "or"]) -> None:
        self.text = TextFilter(VIEW_ID, "text", operator)
        self.boolean = BooleanFilter(VIEW_ID, "boolean", operator)
        self.float32 = FloatFilter(VIEW_ID, "float32", operator)
        self.float64 = FloatFilter(VIEW_ID, "float64", operator)
        self.int32 = IntegerFilter(VIEW_ID, "int32", operator)
        self.int64 = IntegerFilter(VIEW_ID, "int64", operator)
        self.timestamp = DateTimeFilter(VIEW_ID, "timestamp", operator)
        self.date_ = DateFilter(VIEW_ID, "date", operator)
        super().__init__(
            [
                self.text,
                self.boolean,
                self.float32,
                self.float64,
                self.int32,
                self.int64,
                self.timestamp,
                self.date_,
            ],
            operator,
        )


@pytest.fixture
def example_filters() -> ExampleFilter:
    return ExampleFilter(operator="and")


def _get_property_path(property: str) -> list[str]:
    view_id = VIEW_ID
    return [view_id.space, f"{view_id.external_id}/{view_id.version}", property]


class TestDataTypeFilters:
    def test_boolean_filter(self, example_filters: ExampleFilter) -> None:
        boolean_filter = example_filters.boolean
        boolean_filter.equals(True)
        assert boolean_filter.dump() == {"equals": {"property": _get_property_path("boolean"), "value": True}}

    def test_float_filter_equals(self, example_filters: ExampleFilter) -> None:
        float_filter = example_filters.float32
        float_filter.equals(3.14)
        assert float_filter.dump() == {"equals": {"property": _get_property_path("float32"), "value": 3.14}}

    def test_float_filter_range(self, example_filters: ExampleFilter) -> None:
        float_filter = example_filters.float64
        float_filter.greater_than(1.0).less_than(10.0)
        assert float_filter.dump(exclude_none=True) == {
            "range": {"property": _get_property_path("float64"), "gt": 1.0, "lt": 10.0}
        }

    def test_float_filter_range_ge_le(self, example_filters: ExampleFilter) -> None:
        float_filter = example_filters.float64
        float_filter.greater_than_or_equals(2.5).less_than_or_equals(7.5)
        assert float_filter.dump(exclude_none=True) == {
            "range": {"property": _get_property_path("float64"), "gte": 2.5, "lte": 7.5}
        }

    def test_integer_filter_equals(self, example_filters: ExampleFilter) -> None:
        int_filter = example_filters.int32
        int_filter.equals(42)
        assert int_filter.dump() == {"equals": {"property": _get_property_path("int32"), "value": 42}}

    def test_integer_filter_range(self, example_filters: ExampleFilter) -> None:
        int_filter = example_filters.int64
        int_filter.greater_than_or_equals(0).less_than_or_equals(100)
        assert int_filter.dump(exclude_none=True) == {
            "range": {"property": _get_property_path("int64"), "gte": 0, "lte": 100}
        }

    def test_integer_filter_range_ge_lt(self, example_filters: ExampleFilter) -> None:
        int_filter = example_filters.int64
        int_filter.greater_than(10).less_than(50)
        assert int_filter.dump(exclude_none=True) == {
            "range": {"property": _get_property_path("int64"), "gt": 10, "lt": 50}
        }

    def test_datetime_filter_equals(self, example_filters: ExampleFilter) -> None:
        dt = datetime(2024, 1, 15, 12, 30, 0)
        datetime_filter = example_filters.timestamp
        datetime_filter.equals(dt)
        assert datetime_filter.dump() == {
            "equals": {"property": _get_property_path("timestamp"), "value": dt.isoformat(timespec="milliseconds")}
        }

    def test_datetime_filter_range(self, example_filters: ExampleFilter) -> None:
        start = datetime(2024, 1, 1)
        end = datetime(2024, 12, 31)
        datetime_filter = example_filters.timestamp
        datetime_filter.greater_than_or_equals(start).less_than(end)
        assert datetime_filter.dump(exclude_none=True) == {
            "range": {
                "property": _get_property_path("timestamp"),
                "gte": start.isoformat(timespec="milliseconds"),
                "lt": end.isoformat(timespec="milliseconds"),
            }
        }

    def test_datetime_filter_range_with_str(self, example_filters: ExampleFilter) -> None:
        start = "2024-01-01T00:00:00.000Z"
        end = "2024-12-31T23:59:59.999Z"
        datetime_filter = example_filters.timestamp
        datetime_filter.greater_than(start).less_than_or_equals(end)
        assert datetime_filter.dump(exclude_none=True) == {
            "range": {
                "property": _get_property_path("timestamp"),
                "gt": start,
                "lte": end,
            }
        }

    def test_date_filter_equals(self, example_filters: ExampleFilter) -> None:
        d = date(2024, 1, 15)
        date_filter = example_filters.date_
        date_filter.equals(d)
        assert date_filter.dump() == {"equals": {"property": _get_property_path("date"), "value": d.isoformat()}}

    def test_date_filter_range(self, example_filters: ExampleFilter) -> None:
        start = date(2024, 1, 1)
        end = date(2024, 12, 31)
        date_filter = example_filters.date_
        date_filter.greater_than_or_equals(start).less_than_or_equals(end)
        assert date_filter.dump(exclude_none=True) == {
            "range": {"property": _get_property_path("date"), "lte": end.isoformat(), "gte": start.isoformat()}
        }

    def test_date_filter_range_with_str(self, example_filters: ExampleFilter) -> None:
        start = "2024-01-01"
        end = "2024-12-31"
        date_filter = example_filters.date_
        date_filter.greater_than(start).less_than(end)
        assert date_filter.dump(exclude_none=True) == {
            "range": {"property": _get_property_path("date"), "gt": start, "lt": end}
        }

    def test_text_filter_equals(self, example_filters: ExampleFilter) -> None:
        text_filter = example_filters.text
        text_filter.equals("hello")
        assert text_filter.dump() == {"equals": {"property": _get_property_path("text"), "value": "hello"}}

    def test_text_filter_prefix(self, example_filters: ExampleFilter) -> None:
        text_filter = example_filters.text
        text_filter.prefix("test")
        assert text_filter.dump() == {"prefix": {"property": _get_property_path("text"), "value": "test"}}

    def test_text_filter_in(self, example_filters: ExampleFilter) -> None:
        text_filter = example_filters.text
        text_filter.in_(["value1", "value2", "value3"])
        assert text_filter.dump() == {
            "in": {"property": _get_property_path("text"), "values": ["value1", "value2", "value3"]}
        }

    def test_text_filter_multiple_conditions(self, example_filters: ExampleFilter) -> None:
        text_filter = example_filters.text
        text_filter.prefix("start").equals("exact")
        assert text_filter.dump() == {
            "and": [
                {"prefix": {"property": _get_property_path("text"), "value": "start"}},
                {"equals": {"property": _get_property_path("text"), "value": "exact"}},
            ]
        }


class TestFilterContainer:
    def test_null_filter_container(self) -> None:
        assert FilterContainer([], "or").as_filter() is None

    def test_empty_filter_container(self) -> None:
        assert ExampleFilter(operator="and").as_filter() is None

    def test_single_filter(self, example_filters: ExampleFilter) -> None:
        example_filters.int32.equals(5)
        single_filter = example_filters.as_filter()
        assert single_filter is not None
        assert FilterAdapter.dump_python(single_filter) == {
            "equals": {"property": _get_property_path("int32"), "value": 5}
        }

    def test_combined_filters(self, example_filters: ExampleFilter) -> None:
        example_filters.text.equals("example")
        example_filters.int32.greater_than(10)
        combined_filter = example_filters.as_filter()
        assert combined_filter is not None
        assert FilterAdapter.dump_python(combined_filter, exclude_none=True) == {
            "and": [
                {"equals": {"property": _get_property_path("text"), "value": "example"}},
                {"range": {"property": _get_property_path("int32"), "gt": 10}},
            ]
        }
