"""Data model validation logic.

Validates data models and views for issues that would affect code generation.
"""

from collections.abc import Sequence

from cognite.pygen._client.models import (
    ContainerDirectReference,
    DataModelResponse,
    DirectNodeRelation,
    ViewCorePropertyResponse,
    ViewDirectReference,
    ViewReference,
    ViewResponse,
)
from cognite.pygen._client.models._view_property import (
    MultiReverseDirectRelationPropertyResponse,
    ReverseDirectRelationProperty,
    SingleReverseDirectRelationPropertyResponse,
)

from ._issues import DirectRelationMissingSource, NameConflict, ReverseDirectRelationMissingTarget
from ._reserved_words import is_reserved
from ._result import ValidationResult


def validate_data_model(data_model: DataModelResponse, views: Sequence[ViewResponse]) -> ValidationResult:
    """Validate a data model and its views.

    Args:
        data_model: The data model to validate.
        views: The views belonging to this data model.

    Returns:
        ValidationResult containing all detected issues.
    """
    return validate_views(views)


def validate_views(views: Sequence[ViewResponse]) -> ValidationResult:
    """Validate a collection of views.

    Checks for:
    - Reverse direct relations pointing to non-existent targets
    - Direct relations without source views
    - Name conflicts with reserved words

    Args:
        views: The views to validate.

    Returns:
        ValidationResult containing all detected issues.
    """
    result = ValidationResult()

    # Build lookup for quick access
    view_by_ref = {view.as_reference(): view for view in views}
    property_lookup = _build_property_lookup(views)

    for view in views:
        view_ref = view.as_reference()

        # Validate class name
        _check_name_conflict(result, view_ref, view.external_id, "class")

        for prop_name, prop in view.properties.items():
            # Validate property name
            _check_name_conflict(result, view_ref, prop_name, "field")

            # Check reverse direct relations
            if isinstance(
                prop, SingleReverseDirectRelationPropertyResponse | MultiReverseDirectRelationPropertyResponse
            ):
                _validate_reverse_direct_relation(result, view_ref, prop_name, prop, view_by_ref, property_lookup)

            # Check direct relations without source
            elif isinstance(prop, ViewCorePropertyResponse) and isinstance(prop.type, DirectNodeRelation):
                if prop.type.source is None:
                    result.add(DirectRelationMissingSource(view=view_ref, property_name=prop_name))

    return result


def _build_property_lookup(views: Sequence[ViewResponse]) -> dict[tuple[str, str, str, str], bool]:
    """Build a lookup of (space, external_id, version, property_name) -> exists.

    For container references, we use (space, external_id, "", property_name).
    """
    lookup: dict[tuple[str, str, str, str], bool] = {}

    for view in views:
        for prop_name in view.properties:
            # View-based lookup
            lookup[(view.space, view.external_id, view.version, prop_name)] = True

    return lookup


def _validate_reverse_direct_relation(
    result: ValidationResult,
    view_ref: ViewReference,
    prop_name: str,
    prop: ReverseDirectRelationProperty,
    view_by_ref: dict[ViewReference, ViewResponse],
    property_lookup: dict[tuple[str, str, str, str], bool],
) -> None:
    """Validate that a reverse direct relation's target exists."""
    through = prop.through

    # Determine the target view and property
    if isinstance(through, ViewDirectReference):
        target_ref = through.source
        target_prop = through.identifier
        lookup_key = (target_ref.space, target_ref.external_id, target_ref.version, target_prop)
    elif isinstance(through, ContainerDirectReference):
        # For container references, we need to find which view(s) use this container/property
        # This is more complex - we check if the source view exists and has matching properties
        target_ref = prop.source
        target_prop = through.identifier
        lookup_key = (target_ref.space, target_ref.external_id, target_ref.version, target_prop)
    else:
        return

    # Check if target exists
    if lookup_key not in property_lookup:
        # Also check if the view exists at all
        if isinstance(through, ViewDirectReference) and target_ref not in view_by_ref:
            result.add(
                ReverseDirectRelationMissingTarget(
                    view=view_ref,
                    property_name=prop_name,
                    target_view=target_ref,
                    target_property=target_prop,
                )
            )
        elif isinstance(through, ContainerDirectReference):
            # For container references, check if the source view exists
            if prop.source not in view_by_ref:
                result.add(
                    ReverseDirectRelationMissingTarget(
                        view=view_ref,
                        property_name=prop_name,
                        target_view=prop.source,
                        target_property=target_prop,
                    )
                )


def _check_name_conflict(
    result: ValidationResult,
    view_ref: ViewReference | None,
    name: str,
    context: str,
) -> None:
    """Check if a name conflicts with reserved words."""
    reserved, reason = is_reserved(name, context)
    if reserved:
        result.add(
            NameConflict(
                view=view_ref,
                name=name,
                context=context,  # type: ignore[arg-type]
                reserved_in=reason,
            )
        )
