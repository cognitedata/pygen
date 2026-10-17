"""Generation edge cases found by generating the TypeScript SDK for CogniteCore and a customer-style model."""

from pathlib import Path
from typing import Any

import pytest

from cognite.pygen._client.models import DataModelResponseWithViews
from cognite.pygen._generator.typescript import TypeScriptGenerator


def _text_property(container: str, identifier: str) -> dict[str, Any]:
    return {
        "constraintState": {},
        "type": {"type": "text", "list": False, "collation": "ucs_basic"},
        "container": {"type": "container", "space": "my_space", "externalId": container},
        "containerPropertyIdentifier": identifier,
        "immutable": False,
        "nullable": True,
        "autoIncrement": False,
        "description": None,
        "name": identifier,
    }


def _view(external_id: str, used_for: str, properties: dict[str, Any]) -> dict[str, Any]:
    # No "filter" key: the API and the CLI leave it out when a view has no filter.
    return {
        "space": "my_space",
        "externalId": external_id,
        "version": "v1",
        "createdTime": 0,
        "lastUpdatedTime": 0,
        "writable": True,
        "queryable": True,
        "usedFor": used_for,
        "isGlobal": False,
        "mappedContainers": [],
        "name": external_id,
        "description": None,
        "implements": [],
        "properties": properties,
    }


@pytest.fixture(scope="module")
def generated() -> dict[Path, str]:
    model = DataModelResponseWithViews.model_validate(
        {
            "space": "my_space",
            "externalId": "MyModel",
            "version": "v1",
            "createdTime": 0,
            "lastUpdatedTime": 0,
            "isGlobal": False,
            "name": "My model",
            "description": None,
            "views": [
                _view("Describable", "all", {"name": _text_property("Describable", "name")}),
                _view("WorkOrder", "node", {"type": _text_property("WorkOrder", "type")}),
                _view("Cognite3DModel", "node", {"name": _text_property("Cognite3DModel", "name")}),
            ],
        }
    )
    return TypeScriptGenerator(model).generate()


def test_view_used_for_all_is_generated_as_node_view(generated: dict[Path, str]) -> None:
    content = generated[Path("data_classes/describable.ts")]
    assert 'readonly instanceType: "node";' in content


def test_property_named_like_an_instance_filter_gets_suffixed_filter_attribute(generated: dict[Path, str]) -> None:
    data_class = generated[Path("data_classes/workOrder.ts")]
    assert "readonly type_: TextFilter;" in data_class
    assert "readonly type: TextFilter;" not in data_class
    api = generated[Path("_api/_workOrder_api.ts")]
    assert "filter.type_.equalsOrIn(options.type ?? null);" in api


def test_api_files_import_instance_id(generated: dict[Path, str]) -> None:
    for path, content in generated.items():
        if path.parts[0] == "_api" and path.name != "index.ts":
            assert 'import type { InstanceId, ViewReference } from "../instance_api/types/references.ts";' in content


def test_view_constant_name_is_the_same_in_data_class_and_index(generated: dict[Path, str]) -> None:
    data_class = generated[Path("data_classes/cognite3DModel.ts")]
    index = generated[Path("data_classes/index.ts")]
    assert "export const COGNITE_3_D_MODEL_VIEW" in data_class
    assert "COGNITE_3_D_MODEL_VIEW," in index


def test_data_class_index_re_exports_interfaces_as_types(generated: dict[Path, str]) -> None:
    # Vite projects compile with `isolatedModules`, where re-exporting an interface without `export type` is TS1205.
    index = generated[Path("data_classes/index.ts")]
    types = 'export type {\n  WorkOrderWrite,\n  WorkOrder,\n} from "./workOrder.ts";'
    values = (
        "export {\n  WORK_ORDER_VIEW,\n  workOrderAsWrite,\n  WorkOrderList,\n  WorkOrderFilter,\n"
        '} from "./workOrder.ts";'
    )
    assert types in index
    assert values in index
