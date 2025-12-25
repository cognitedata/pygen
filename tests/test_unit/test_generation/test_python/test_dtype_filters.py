import pytest

from cognite.pygen._generation.python.example._data_class import PrimitiveNullable, PrimitiveNullableFilter
from cognite.pygen._generation.python.instance_api.models.filters import FilterAdapter


@pytest.fixture(scope="module")
def primitive_filter() -> PrimitiveNullableFilter:
    return PrimitiveNullableFilter(operator="and")


def _get_property_path(property: str) -> list[str]:
    view_id = PrimitiveNullable._view_id
    return [view_id.space, f"{view_id.external_id}/{view_id.version}", property]


class TestDataTypeFilters:
    def test_boolean_filter(self, primitive_filter: PrimitiveNullableFilter) -> None:
        boolean_filter = primitive_filter.boolean
        boolean_filter.equals(True)
        assert FilterAdapter.dump_python(boolean_filter.as_filter()) == {
            "equals": {"property": _get_property_path("boolean"), "value": True}
        }
