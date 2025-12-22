"""Tests for the validation module."""

from typing import Any

import pytest

from cognite.pygen._client.models import ViewReference, ViewResponse
from cognite.pygen._client.validation import (
    DATA_CLASS_RESERVED,
    FIELD_RESERVED,
    FILE_RESERVED,
    PARAMETER_RESERVED,
    DirectRelationMissingSource,
    NameConflict,
    ReverseDirectRelationMissingTarget,
    ValidationResult,
    is_reserved,
    validate_views,
)


class TestReservedWords:
    def test_python_keyword_is_reserved_in_all_contexts(self) -> None:
        for context in ["field", "parameter", "class", "file"]:
            reserved, reason = is_reserved("if", context)
            assert reserved
            assert reason == "Python keyword"

    def test_python_builtin_is_reserved(self) -> None:
        reserved, reason = is_reserved("print", "field")
        assert reserved
        assert reason == "Python builtin"

    def test_pydantic_method_is_reserved_for_field(self) -> None:
        reserved, reason = is_reserved("model_dump", "field")
        assert reserved
        assert reason == "Pydantic BaseModel attribute"

    def test_pydantic_method_not_reserved_for_parameter(self) -> None:
        # model_dump is not reserved for parameters, only for fields
        reserved, _reason = is_reserved("model_dump", "parameter")
        # model_dump is not a Python builtin or keyword, so it should not be reserved
        assert not reserved

    def test_sdk_field_is_reserved(self) -> None:
        reserved, reason = is_reserved("external_id", "field")
        assert reserved
        assert reason == "SDK data class field"

    def test_sdk_parameter_is_reserved(self) -> None:
        reserved, reason = is_reserved("limit", "parameter")
        assert reserved
        assert reason == "SDK parameter"

    def test_sdk_class_name_is_reserved(self) -> None:
        reserved, reason = is_reserved("DomainModel", "class")
        assert reserved
        assert reason == "SDK class name"

    def test_sdk_file_name_is_reserved(self) -> None:
        reserved, reason = is_reserved("__init__", "file")
        assert reserved
        assert reason == "SDK file name"

    def test_regular_name_not_reserved(self) -> None:
        reserved, reason = is_reserved("my_property", "field")
        assert not reserved
        assert reason == ""

    def test_reserved_sets_not_empty(self) -> None:
        assert len(FIELD_RESERVED) > 0
        assert len(PARAMETER_RESERVED) > 0
        assert len(DATA_CLASS_RESERVED) > 0
        assert len(FILE_RESERVED) > 0


class TestValidationResult:
    def test_empty_result(self) -> None:
        result = ValidationResult()
        assert not result.has_issues
        assert result.issue_count == 0
        assert result.all_issues == []
        assert "No validation issues" in result.summary()

    def test_add_missing_reverse_target(self) -> None:
        result = ValidationResult()
        issue = ReverseDirectRelationMissingTarget(
            view=ViewReference(space="sp", external_id="view1", version="v1"),
            property_name="prop1",
            target_view=ViewReference(space="sp", external_id="missing_view", version="v1"),
            target_property="related",
        )
        result.add(issue)
        assert result.has_issues
        assert result.issue_count == 1
        assert len(result.missing_reverse_targets) == 1
        assert "missing targets" in result.summary()

    def test_add_missing_direct_source(self) -> None:
        result = ValidationResult()
        issue = DirectRelationMissingSource(
            view=ViewReference(space="sp", external_id="view1", version="v1"),
            property_name="prop1",
        )
        result.add(issue)
        assert result.has_issues
        assert len(result.missing_direct_sources) == 1

    def test_add_name_conflict(self) -> None:
        result = ValidationResult()
        issue = NameConflict(
            view=ViewReference(space="sp", external_id="view1", version="v1"),
            name="if",
            context="field",
            reserved_in="Python keyword",
        )
        result.add(issue)
        assert result.has_issues
        assert len(result.name_conflicts) == 1

    def test_all_issues_combines_all_types(self) -> None:
        result = ValidationResult()
        result.add(
            DirectRelationMissingSource(
                view=ViewReference(space="sp", external_id="v1", version="v1"),
                property_name="p1",
            )
        )
        result.add(
            NameConflict(
                view=ViewReference(space="sp", external_id="v2", version="v1"),
                name="if",
                context="field",
                reserved_in="Python keyword",
            )
        )
        assert result.issue_count == 2
        assert len(result.all_issues) == 2


@pytest.fixture
def simple_view_data() -> dict[str, Any]:
    return {
        "space": "my_space",
        "externalId": "my_view",
        "version": "v1",
        "createdTime": 1625247600000,
        "lastUpdatedTime": 1625247600000,
        "isGlobal": False,
        "writable": True,
        "queryable": True,
        "usedFor": "node",
        "filter": None,
        "implements": None,
        "properties": {
            "name": {
                "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
                "containerPropertyIdentifier": "name",
                "type": {"type": "text"},
                "nullable": True,
                "immutable": False,
                "constraintState": {"nullability": "current"},
            },
        },
        "mappedContainers": [{"space": "my_space", "externalId": "my_container", "type": "container"}],
    }


class TestValidateViews:
    def test_valid_view_no_issues(self, simple_view_data: dict[str, Any]) -> None:
        view = ViewResponse.model_validate(simple_view_data)
        result = validate_views([view])
        assert not result.has_issues

    def test_direct_relation_without_source(self, simple_view_data: dict[str, Any]) -> None:
        simple_view_data["properties"]["related"] = {
            "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
            "containerPropertyIdentifier": "related",
            "type": {"type": "direct"},  # No source
            "nullable": True,
            "immutable": False,
            "constraintState": {"nullability": "current"},
        }
        view = ViewResponse.model_validate(simple_view_data)
        result = validate_views([view])
        assert result.has_issues
        assert len(result.missing_direct_sources) == 1
        assert result.missing_direct_sources[0].property_name == "related"

    def test_direct_relation_with_source_no_issue(self, simple_view_data: dict[str, Any]) -> None:
        simple_view_data["properties"]["related"] = {
            "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
            "containerPropertyIdentifier": "related",
            "type": {
                "type": "direct",
                "source": {"space": "my_space", "externalId": "target_view", "version": "v1"},
            },
            "nullable": True,
            "immutable": False,
            "constraintState": {"nullability": "current"},
        }
        view = ViewResponse.model_validate(simple_view_data)
        result = validate_views([view])
        # No issue for direct relation with source
        assert len(result.missing_direct_sources) == 0

    def test_reserved_property_name(self, simple_view_data: dict[str, Any]) -> None:
        simple_view_data["properties"]["model_dump"] = {
            "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
            "containerPropertyIdentifier": "model_dump",
            "type": {"type": "text"},
            "nullable": True,
            "immutable": False,
            "constraintState": {"nullability": "current"},
        }
        view = ViewResponse.model_validate(simple_view_data)
        result = validate_views([view])
        assert result.has_issues
        assert len(result.name_conflicts) == 1
        assert result.name_conflicts[0].name == "model_dump"
        assert result.name_conflicts[0].context == "field"

    def test_reserved_class_name(self) -> None:
        view_data = {
            "space": "my_space",
            "externalId": "DomainModel",  # Reserved class name
            "version": "v1",
            "createdTime": 1625247600000,
            "lastUpdatedTime": 1625247600000,
            "isGlobal": False,
            "writable": True,
            "queryable": True,
            "usedFor": "node",
            "filter": None,
            "implements": None,
            "properties": {},
            "mappedContainers": [],
        }
        view = ViewResponse.model_validate(view_data)
        result = validate_views([view])
        assert result.has_issues
        assert len(result.name_conflicts) == 1
        assert result.name_conflicts[0].context == "class"

    def test_reverse_direct_relation_with_missing_source_view(self) -> None:
        view_data = {
            "space": "my_space",
            "externalId": "my_view",
            "version": "v1",
            "createdTime": 1625247600000,
            "lastUpdatedTime": 1625247600000,
            "isGlobal": False,
            "writable": True,
            "queryable": True,
            "usedFor": "node",
            "filter": None,
            "implements": None,
            "properties": {
                "reverse_rel": {
                    "connectionType": "single_reverse_direct_relation",
                    "source": {
                        "space": "my_space",
                        "externalId": "missing_view",  # This view doesn't exist
                        "version": "v1",
                        "type": "view",
                    },
                    "through": {
                        "source": {"space": "my_space", "externalId": "my_container", "type": "container"},
                        "identifier": "related",
                    },
                    "targetsList": False,
                },
            },
            "mappedContainers": [],
        }
        view = ViewResponse.model_validate(view_data)
        result = validate_views([view])
        assert result.has_issues
        assert len(result.missing_reverse_targets) == 1
        assert result.missing_reverse_targets[0].property_name == "reverse_rel"

    def test_reverse_direct_relation_with_existing_source_view(self) -> None:
        # Create two views: the main view and the source view for the reverse relation
        main_view_data = {
            "space": "my_space",
            "externalId": "main_view",
            "version": "v1",
            "createdTime": 1625247600000,
            "lastUpdatedTime": 1625247600000,
            "isGlobal": False,
            "writable": True,
            "queryable": True,
            "usedFor": "node",
            "filter": None,
            "implements": None,
            "properties": {
                "reverse_rel": {
                    "connectionType": "single_reverse_direct_relation",
                    "source": {"space": "my_space", "externalId": "source_view", "version": "v1", "type": "view"},
                    "through": {
                        "source": {"space": "my_space", "externalId": "source_view", "version": "v1", "type": "view"},
                        "identifier": "related",
                    },
                    "targetsList": False,
                },
            },
            "mappedContainers": [],
        }
        source_view_data = {
            "space": "my_space",
            "externalId": "source_view",
            "version": "v1",
            "createdTime": 1625247600000,
            "lastUpdatedTime": 1625247600000,
            "isGlobal": False,
            "writable": True,
            "queryable": True,
            "usedFor": "node",
            "filter": None,
            "implements": None,
            "properties": {
                "related": {
                    "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
                    "containerPropertyIdentifier": "related",
                    "type": {"type": "direct"},
                    "nullable": True,
                    "immutable": False,
                    "constraintState": {"nullability": "current"},
                },
            },
            "mappedContainers": [{"space": "my_space", "externalId": "my_container", "type": "container"}],
        }
        main_view = ViewResponse.model_validate(main_view_data)
        source_view = ViewResponse.model_validate(source_view_data)
        result = validate_views([main_view, source_view])
        # No missing reverse target issue because source_view exists with "related" property
        assert len(result.missing_reverse_targets) == 0

    def test_multiple_issues_in_single_view(self, simple_view_data: dict[str, Any]) -> None:
        # Add reserved property name and direct relation without source
        simple_view_data["properties"]["print"] = {  # Reserved
            "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
            "containerPropertyIdentifier": "print",
            "type": {"type": "text"},
            "nullable": True,
            "immutable": False,
            "constraintState": {"nullability": "current"},
        }
        simple_view_data["properties"]["related"] = {
            "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
            "containerPropertyIdentifier": "related",
            "type": {"type": "direct"},  # No source
            "nullable": True,
            "immutable": False,
            "constraintState": {"nullability": "current"},
        }
        view = ViewResponse.model_validate(simple_view_data)
        result = validate_views([view])
        assert result.issue_count >= 2
        assert len(result.name_conflicts) >= 1
        assert len(result.missing_direct_sources) == 1


class TestIssueStrRepresentations:
    def test_reverse_direct_relation_missing_target_str(self) -> None:
        issue = ReverseDirectRelationMissingTarget(
            view=ViewReference(space="sp", external_id="view1", version="v1"),
            property_name="reverse_prop",
            target_view=ViewReference(space="sp", external_id="missing_view", version="v1"),
            target_property="related",
        )
        msg = str(issue)
        assert "view1.reverse_prop" in msg
        assert "missing_view.related" in msg
        assert "excluded" in msg.lower()

    def test_direct_relation_missing_source_str(self) -> None:
        issue = DirectRelationMissingSource(
            view=ViewReference(space="sp", external_id="view1", version="v1"),
            property_name="direct_prop",
        )
        msg = str(issue)
        assert "view1.direct_prop" in msg
        assert "generic node reference" in msg.lower()

    def test_name_conflict_str_with_view(self) -> None:
        issue = NameConflict(
            view=ViewReference(space="sp", external_id="view1", version="v1"),
            name="if",
            context="field",
            reserved_in="Python keyword",
        )
        msg = str(issue)
        assert "if" in msg
        assert "view1" in msg
        assert "Python keyword" in msg
        assert "underscore" in msg.lower()

    def test_name_conflict_str_without_view(self) -> None:
        issue = NameConflict(
            view=None,
            name="limit",
            context="parameter",
            reserved_in="SDK parameter",
        )
        msg = str(issue)
        assert "limit" in msg
        assert "SDK parameter" in msg
