from typing import get_args

from cognite.pygen._client.models.constraints import Constraint, ConstraintDefinition
from cognite.pygen._client.models.data_types import DataType, PropertyTypeDefinition
from cognite.pygen._client.models.indexes import Index, IndexDefinition
from cognite.pygen._utils.collection import humanize_collection
from tests.utils import get_concrete_subclasses


class TestContainer:
    def test_all_indexes_are_in_union(self) -> None:
        all_indices = get_concrete_subclasses(IndexDefinition, exclude_direct_abc_inheritance=True)
        all_union_indices = get_args(Index.__args__[0])
        missing = set(all_indices) - set(all_union_indices)
        assert not missing, (
            f"The following IndexDefinition subclasses are "
            f"missing from the Index union: {humanize_collection([cls.__name__ for cls in missing])}"
        )

    def test_all_constraints_are_in_union(self) -> None:
        all_constraints = get_concrete_subclasses(ConstraintDefinition, exclude_direct_abc_inheritance=True)
        all_union_constraints = get_args(Constraint.__args__[0])
        missing = set(all_constraints) - set(all_union_constraints)
        assert not missing, (
            f"The following ConstraintDefinition subclasses are "
            f"missing from the Constraint union: {humanize_collection([cls.__name__ for cls in missing])}"
        )

    def test_all_property_types_are_in_union(self) -> None:
        all_property_types = get_concrete_subclasses(PropertyTypeDefinition, exclude_direct_abc_inheritance=True)
        all_union_property_types = get_args(DataType.__args__[0])
        missing = set(all_property_types) - set(all_union_property_types)
        assert not missing, (
            f"The following PropertyTypeDefinition subclasses are "
            f"missing from the DataType union: {humanize_collection([cls.__name__ for cls in missing])}"
        )


class TestView: ...
