import importlib
import sys
import warnings
from contextlib import contextmanager
from pathlib import Path

from cognite.client import data_modeling as dm
from pydantic import BaseModel

from cognite.pygen import generate_sdk
from tests.constants import CORE_SDK


@contextmanager
def append_to_sys_path(path: str):
    sys.path.append(path)
    try:
        yield
    finally:
        sys.path.remove(path)


class TestGenerateSDK:
    def test_generate_sdk_from_with_view_named_Field(self, tmp_path):
        client_name = "FieldClient"
        top_level_package = "field_model"

        generate_sdk(
            DATA_MODEL_WITH_VIEW_NAMED_FIELD,
            top_level_package=top_level_package,
            output_dir=tmp_path / top_level_package,
            overwrite=True,
            client_name=client_name,
            default_instance_space="field_space",
        )

        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module

    def test_generate_sdk_from_view_with_direct_relation_to_view_with_property_of_type_Field(self, tmp_path):
        client_name = "FieldsClient"
        top_level_package = "fields_model"

        generate_sdk(
            DATA_MODEL_WITH_VIEW_PROPERTY_OF_TYPE_FIELD,
            top_level_package=top_level_package,
            output_dir=tmp_path / top_level_package,
            overwrite=True,
            client_name=client_name,
            default_instance_space="fields-space",
        )

        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module

    def test_generate_sdk_with_view_without_properties(self, tmp_path):
        client_name = "NoPropertiesClient"
        top_level_package = "no_properties_model"

        generate_sdk(
            DATA_MODEL_WITH_VIEW_WITHOUT_PROPERTIES,
            top_level_package=top_level_package,
            output_dir=tmp_path / top_level_package,
            overwrite=True,
            client_name=client_name,
            default_instance_space="no_properties_space",
        )

        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module

    def test_generate_sdk_with_illegal_property_names(self, tmp_path: Path) -> None:
        client_name = "IllegalPropertyNamesClient"
        top_level_package = "illegal_property_names_model"

        generate_sdk(
            DATA_MODEL_WITH_ILLEGAL_PROPERTY_NAMES,
            top_level_package=top_level_package,
            output_dir=tmp_path / top_level_package,
            overwrite=True,
            client_name=client_name,
            default_instance_space="illegal_property_names_space",
        )

        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module

    def test_generate_sdk_with_reverse_direct_relation_without_target(self, tmp_path: Path) -> None:
        client_name = "ReverseDirectRelationClient"
        top_level_package = "reverse_direct_relation_model"

        generate_sdk(
            DATA_MODEL_WITH_REVERSE_DIRECT_RELATION_WITHOUT_TARGET,
            top_level_package=top_level_package,
            output_dir=tmp_path / top_level_package,
            overwrite=True,
            client_name=client_name,
            default_instance_space="reverse_direct_relation_space",
        )

        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module

    def test_generate_sdk_edge_source_outside_model(self, tmp_path: Path) -> None:
        client_name = "HydroClient"
        top_level_package = "hydro_model"

        generate_sdk(
            DATA_MODEL_SOURCE_OUTSIDE_MODEL,
            top_level_package=top_level_package,
            output_dir=tmp_path / top_level_package,
            overwrite=True,
            client_name=client_name,
            default_instance_space="hydro_energi_watercourse_type_space",
        )

        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module

    def test_generate_sdk_direct_relation_missing_source(self, tmp_path: Path) -> None:
        client_name = "HydroClient"
        top_level_package = "hydro_model"

        generate_sdk(
            DATA_MODEL_DIRECT_RELATION_MISSING_SOURCE,
            top_level_package=top_level_package,
            output_dir=tmp_path / top_level_package,
            overwrite=True,
            client_name=client_name,
            default_instance_space="hydro_energi_watercourse_type_space",
        )

        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module

    def test_generate_sdk_reverse_direct_relation_through_container(self, tmp_path: Path) -> None:
        client_name = "ReverseDirectRelationClient"
        top_level_package = "reverse_direct_relation_model"

        with warnings.catch_warnings(record=True) as logger:
            warnings.simplefilter("always")
            generate_sdk(
                DATA_MODEL_REVERSE_DIRECT_RELATION_THROUGH_CONTAINER,
                top_level_package=top_level_package,
                output_dir=tmp_path / top_level_package,
                overwrite=True,
                client_name=client_name,
                default_instance_space="reverse_direct_relation_space",
            )
            assert len(logger) == 0
        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module

    def test_generate_sdk_exclude_views(self, tmp_path: Path) -> None:
        core = CORE_SDK.load_data_model()
        exclude_views: set[str | dm.ViewId] = {
            view.as_id() for view in core.views if view.external_id != "CogniteAsset"
        }
        top_level_package = "core_sdk"
        client_name = "CoreClient"

        with warnings.catch_warnings(record=True) as logger:
            warnings.simplefilter("always")
            generate_sdk(
                core,
                top_level_package=top_level_package,
                output_dir=tmp_path / top_level_package,
                overwrite=True,
                client_name=client_name,
                default_instance_space="my_space",
                exclude_views=exclude_views,
            )
            assert len(logger) == 0
        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module

    def test_generate_sdk_with_edge_view_with_reverse_direct_relation(self, tmp_path: Path) -> None:
        top_level_package = "reverse_direct_relation_model"
        client_name = "ReverseDirectRelationClient"

        generate_sdk(
            DATA_MODEL_REVERSE_DIRECT_RELATION_IN_EDGE_VIEW,
            top_level_package=top_level_package,
            output_dir=tmp_path / top_level_package,
            overwrite=True,
            client_name=client_name,
            default_instance_space="the_instance_space",
        )

        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module

    def test_generate_sdk_overwriting_parent_property(self, tmp_path: Path) -> None:
        top_level_package = "overwriting_parent_property_model"
        client_name = "OverwritingParentPropertyClient"

        generate_sdk(
            DATA_MODEL_WITH_OVERWRITING_PARENT_PROPERTY,
            top_level_package=top_level_package,
            output_dir=tmp_path / top_level_package,
            overwrite=True,
            client_name=client_name,
            default_instance_space="my_instances",
        )

        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module
            data_class_module = vars(importlib.import_module(f"{top_level_package}.data_classes"))
            assert "MyAsset" in data_class_module
            my_asset = data_class_module["MyAsset"]
            assert issubclass(my_asset, BaseModel)
            assert "parent" in my_asset.model_fields
            parent_field = my_asset.model_fields["parent"]
            assert "MyAsset" in str(parent_field.annotation)

    def test_generate_sdk_overwriting_reverse_direct_relation(self, tmp_path: Path) -> None:
        top_level_package = "overwriting_reverse_direct_relation_model"
        client_name = "OverwritingReverseDirectRelationClient"

        printed_messages: list[str] = []

        def logger(message: str) -> None:
            printed_messages.append(message)

        generate_sdk(
            DATA_MODEL_OVERWRITING_REVERSE_DIRECT_RELATION,
            top_level_package=top_level_package,
            output_dir=tmp_path / top_level_package,
            overwrite=True,
            client_name=client_name,
            default_instance_space="my_space",
            logger=logger,
        )
        skipping_messages = [msg for msg in printed_messages if "skipping reverse direct relation" in msg.casefold()]
        assert len(skipping_messages) == 0, f"Unexpected skipping messages: {skipping_messages}"

        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module
            data_class_module = vars(importlib.import_module(f"{top_level_package}.data_classes"))
            assert "Tag" in data_class_module
            tag = data_class_module["Tag"]
            assert issubclass(tag, BaseModel)
            assert "children" in tag.model_fields


DATA_MODEL_WITH_VIEW_PROPERTY_OF_TYPE_FIELD: dm.DataModel = dm.DataModel.load(
    {
        "space": "fields-space",
        "externalId": "Fields",
        "name": "Fields",
        "description": "",
        "version": "1",
        "views": [
            {
                "space": "fields-space",
                "externalId": "Field",
                "name": "Field",
                "implements": [],
                "version": "1",
                "writable": True,
                "usedFor": "node",
                "isGlobal": False,
                "properties": {
                    "name": {
                        "container": {"space": "fields-space", "externalId": "Field"},
                        "containerPropertyIdentifier": "name",
                        "type": {"list": False, "collation": "ucs_basic", "type": "text"},
                        "nullable": True,
                        "immutable": False,
                        "autoIncrement": False,
                        "source": None,
                        "defaultValue": None,
                        "name": "name",
                        "description": None,
                    }
                },
                "lastUpdatedTime": 0,
                "createdTime": 0,
            },
            {
                "space": "fields-space",
                "externalId": "Area",
                "name": "Area",
                "implements": [],
                "version": "1",
                "writable": True,
                "usedFor": "node",
                "isGlobal": False,
                "properties": {
                    "field": {
                        "container": {"space": "fields-space", "externalId": "Area"},
                        "containerPropertyIdentifier": "field",
                        "type": {
                            "list": False,
                            "type": "direct",
                            "source": {"space": "fields-space", "externalId": "Field", "version": "1"},
                        },
                        "nullable": True,
                        "immutable": False,
                        "autoIncrement": False,
                        "defaultValue": None,
                        "name": "field",
                        "description": None,
                    }
                },
                "lastUpdatedTime": 0,
                "createdTime": 0,
            },
            {
                "space": "fields-space",
                "externalId": "Region",
                "name": "Region",
                "implements": [],
                "version": "1",
                "writable": True,
                "usedFor": "node",
                "isGlobal": False,
                "properties": {
                    "area": {
                        "container": {"space": "fields-space", "externalId": "Region"},
                        "containerPropertyIdentifier": "area",
                        "type": {
                            "list": False,
                            "type": "direct",
                            "source": {"space": "fields-space", "externalId": "Area", "version": "1"},
                        },
                        "nullable": True,
                        "immutable": False,
                        "autoIncrement": False,
                        "defaultValue": None,
                        "name": "area",
                        "description": None,
                    }
                },
                "lastUpdatedTime": 0,
                "createdTime": 0,
            },
        ],
        "isGlobal": False,
        "lastUpdatedTime": 0,
        "createdTime": 0,
    }
)


DATA_MODEL_WITH_VIEW_NAMED_FIELD = dm.DataModel(
    space="field_space",
    external_id="FieldModel",
    version="1",
    is_global=False,
    last_updated_time=0,
    created_time=0,
    description=None,
    name=None,
    views=[
        dm.View(
            space="field_space",
            name="Field",
            external_id="Field",
            version="1",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("field_space", "Field"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=False,
                    auto_increment=False,
                    immutable=False,
                )
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
        ),
        dm.View(
            space="field_space",
            name="AnotherType",
            external_id="AnotherType",
            version="1",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("field_space", "Field"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=False,
                    auto_increment=False,
                    immutable=False,
                ),
                "field": dm.MappedProperty(
                    container=dm.ContainerId("field_space", "Field"),
                    container_property_identifier="field",
                    type=dm.DirectRelation(),
                    nullable=True,
                    auto_increment=False,
                    source=dm.ViewId("field_space", "Field", "1"),
                    immutable=False,
                ),
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
        ),
    ],
)

DATA_MODEL_WITH_VIEW_WITHOUT_PROPERTIES = dm.DataModel(
    space="no_properties_space",
    external_id="NoPropertiesModel",
    version="1",
    is_global=False,
    last_updated_time=0,
    created_time=0,
    description=None,
    name=None,
    views=[
        dm.View(
            space="no_properties_space",
            name="NoProperties",
            external_id="NoProperties",
            version="1",
            properties={},
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
        )
    ],
)

DATA_MODEL_WITH_ILLEGAL_PROPERTY_NAMES = dm.DataModel(
    space="illegal_property_names_space",
    external_id="IllegalPropertyNamesModel",
    version="1",
    is_global=False,
    last_updated_time=0,
    created_time=0,
    description=None,
    name=None,
    views=[
        dm.View(
            space="illegal_property_names_space",
            name="IllegalPropertyNames",
            external_id="IllegalPropertyNames",
            version="1",
            properties={
                "name-tag": dm.MappedProperty(
                    container=dm.ContainerId("illegal_property_names_space", "IllegalPropertyNames"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=False,
                    auto_increment=False,
                    immutable=False,
                ),
                "3d-field": dm.MappedProperty(
                    container=dm.ContainerId("illegal_property_names_space", "IllegalPropertyNames"),
                    container_property_identifier="field",
                    type=dm.Text(),
                    nullable=True,
                    auto_increment=False,
                    immutable=False,
                ),
                "class": dm.MappedProperty(
                    container=dm.ContainerId("illegal_property_names_space", "IllegalPropertyNames"),
                    container_property_identifier="class",
                    type=dm.DirectRelation(),
                    nullable=True,
                    auto_increment=False,
                    immutable=False,
                    source=dm.ViewId(
                        space="illegal_property_names_space",
                        external_id="AnotherType",
                        version="1",
                    ),
                ),
                "classes": dm.MappedProperty(
                    container=dm.ContainerId("illegal_property_names_space", "IllegalPropertyNames"),
                    container_property_identifier="classes",
                    type=dm.DirectRelation(is_list=True),
                    nullable=True,
                    auto_increment=False,
                    immutable=False,
                    source=dm.ViewId(
                        space="illegal_property_names_space",
                        external_id="AnotherType",
                        version="1",
                    ),
                ),
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
        ),
        dm.View(
            space="illegal_property_names_space",
            name="AnotherType",
            external_id="AnotherType",
            version="1",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("illegal_property_names_space", "IllegalPropertyNames"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=False,
                    auto_increment=False,
                    immutable=False,
                ),
                "field": dm.MappedProperty(
                    container=dm.ContainerId("illegal_property_names_space", "IllegalPropertyNames"),
                    container_property_identifier="field",
                    type=dm.Text(),
                    nullable=True,
                    auto_increment=False,
                    source=dm.ViewId("illegal_property_names_space", "IllegalPropertyNames", "1"),
                    immutable=False,
                ),
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
        ),
    ],
)

DATA_MODEL_WITH_REVERSE_DIRECT_RELATION_WITHOUT_TARGET = dm.DataModel(
    space="reverse_direct_relation_space",
    external_id="ReverseDirectRelationModel",
    version="1",
    is_global=False,
    last_updated_time=0,
    created_time=0,
    description=None,
    name=None,
    views=[
        dm.View(
            space="reverse_direct_relation_space",
            name="ReverseDirectRelationTarget",
            external_id="ReverseDirectRelationTarget",
            version="1",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("reverse_direct_relation_space", "ReverseDirectRelation"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=False,
                    auto_increment=False,
                    immutable=False,
                ),
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
        ),
        dm.View(
            space="reverse_direct_relation_space",
            name="ReverseDirectRelation",
            external_id="ReverseDirectRelation",
            version="1",
            properties={
                "pointingToNonExisting": dm.MultiReverseDirectRelation(
                    source=dm.ViewId("reverse_direct_relation_space", "ReverseDirectRelationTarget", "1"),
                    through=dm.PropertyId(
                        source=dm.ViewId("reverse_direct_relation_space", "ReverseDirectRelationTarget", "1"),
                        property="NonExisting",
                    ),
                ),
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
        ),
    ],
)

DATA_MODEL_SOURCE_OUTSIDE_MODEL = dm.DataModel(
    space="hydro_energi_watercourse_type_space",
    external_id="HydroModel",
    version="v1",
    is_global=False,
    last_updated_time=1,
    created_time=1,
    description=None,
    name=None,
    views=[
        dm.View(
            space="hydro_energi_watercourse_type_space",
            external_id="CreekIntake",
            version="v1",
            properties={
                "inflow": dm.MultiEdgeConnection(
                    type=dm.DirectRelationReference("inflow", "hydro_energi_watercourse_type_space"),
                    source=dm.ViewId("cdf_cdm", "CogniteTimeSeries", "v1"),
                    direction="outwards",
                    edge_source=None,
                    name=None,
                    description=None,
                )
            },
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
            is_global=False,
        )
    ],
)

DATA_MODEL_DIRECT_RELATION_MISSING_SOURCE = dm.DataModel(
    space="hydro_energi_watercourse_type_space",
    external_id="HydroModel",
    version="v1",
    is_global=False,
    last_updated_time=1,
    created_time=1,
    description=None,
    name=None,
    views=[
        dm.View(
            space="hydro_energi_watercourse_type_space",
            external_id="Reservoir",
            version="v1",
            properties={
                "path": dm.MappedProperty(
                    container=dm.ContainerId(
                        space="cdf_cdm",
                        external_id="CogniteTimeSeries",
                    ),
                    container_property_identifier="path",
                    source=None,
                    name=None,
                    description=None,
                    type=dm.DirectRelation(is_list=True),
                    nullable=True,
                    auto_increment=False,
                    immutable=False,
                ),
            },
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
            is_global=False,
        )
    ],
)
DATA_MODEL_REVERSE_DIRECT_RELATION_THROUGH_CONTAINER = dm.DataModel(
    space="reverse_direct_relation_space",
    external_id="ReverseDirectRelationModel",
    version="1",
    is_global=False,
    last_updated_time=0,
    created_time=0,
    description=None,
    name=None,
    views=[
        dm.View(
            space="reverse_direct_relation_space",
            name="ReverseDirectRelationTarget",
            external_id="ReverseDirectRelationTarget",
            version="1",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("reverse_direct_relation_space", "ReverseDirectRelation"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=False,
                    auto_increment=False,
                    immutable=False,
                ),
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
        ),
        dm.View(
            space="reverse_direct_relation_space",
            name="ReverseDirectRelation",
            external_id="ReverseDirectRelation",
            version="1",
            properties={
                "pointingSourceContainer": dm.MultiReverseDirectRelation(
                    source=dm.ViewId("reverse_direct_relation_space", "ReverseDirectRelationTarget", "1"),
                    through=dm.PropertyId(
                        source=dm.ContainerId("reverse_direct_relation_space", "ReverseDirectRelation"),
                        property="name",
                    ),
                ),
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
        ),
    ],
)

DATA_MODEL_REVERSE_DIRECT_RELATION_IN_EDGE_VIEW = dm.DataModel(
    space="my_space",
    external_id="MyModel",
    version="1",
    is_global=False,
    last_updated_time=0,
    created_time=0,
    description=None,
    name=None,
    views=[
        dm.View(
            space="my_space",
            external_id="WellboreMudRelationship",
            version="1",
            implements=[],
            properties={
                "muds": dm.MultiReverseDirectRelation(
                    source=dm.ViewId("my_space", "Mud", "1"),
                    through=dm.PropertyId(
                        source=dm.ViewId("my_space", "Mud", "1"),
                        property="wellbore",
                    ),
                )
            },
            used_for="all",
            writable=False,
            filter=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            name=None,
            description=None,
        ),
        dm.View(
            space="my_space",
            name="Mud",
            external_id="Mud",
            version="1",
            properties={
                "wellbore": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "MudContainer"),
                    container_property_identifier="wellbore",
                    type=dm.DirectRelation(),
                    nullable=True,
                    auto_increment=False,
                    immutable=False,
                    source=dm.ViewId("my_space", "Wellbore", "1"),
                ),
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
        ),
        dm.View(
            space="my_space",
            name="Wellbore",
            external_id="Wellbore",
            version="1",
            implements=[dm.ViewId("my_space", "WellboreMudRelationship", "1")],
            properties={
                "muds": dm.MultiReverseDirectRelation(
                    source=dm.ViewId("my_space", "Mud", "1"),
                    through=dm.PropertyId(
                        source=dm.ViewId("my_space", "Mud", "1"),
                        property="wellbore",
                    ),
                )
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            writable=True,
            filter=None,
        ),
    ],
)

DATA_MODEL_WITH_OVERWRITING_PARENT_PROPERTY = dm.DataModel(
    space="my_space",
    external_id="MyModel",
    version="1",
    is_global=False,
    last_updated_time=0,
    created_time=0,
    description=None,
    name=None,
    views=[
        dm.View(
            space="my_space",
            name="Parent",
            external_id="CogniteAsset",
            version="1",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "CogniteAsset"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=False,
                    auto_increment=False,
                    immutable=False,
                ),
                "parent": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "CogniteAsset"),
                    container_property_identifier="parent",
                    type=dm.DirectRelation(),
                    nullable=True,
                    auto_increment=False,
                    immutable=False,
                    source=dm.ViewId("my_space", "CogniteAsset", "1"),
                ),
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=None,
            writable=True,
            filter=None,
        ),
        dm.View(
            space="my_space",
            name="MyAsset",
            external_id="MyAsset",
            version="1",
            properties={
                "parent": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "CogniteAsset"),
                    container_property_identifier="parent",
                    type=dm.DirectRelation(),
                    nullable=True,
                    auto_increment=False,
                    immutable=False,
                    source=dm.ViewId("my_space", "MyAsset", "1"),
                ),
                "location": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "MyAsset"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=False,
                    auto_increment=False,
                    immutable=False,
                ),
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            implements=[dm.ViewId("my_space", "CogniteAsset", "1")],
            writable=True,
            filter=None,
        ),
    ],
)

DATA_MODEL_OVERWRITING_REVERSE_DIRECT_RELATION = dm.DataModel(
    space="my_space",
    external_id="MyModel",
    version="1",
    is_global=False,
    last_updated_time=0,
    created_time=0,
    description=None,
    name=None,
    views=[
        dm.View(
            space="my_space",
            external_id="Tag",
            version="v1",
            name="Tag",
            implements=[dm.ViewId("cdf_cdm", "CogniteAsset", "v1")],
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("cdf_cdm", "CogniteDescribable"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=False,
                    auto_increment=False,
                    immutable=False,
                ),
                "parent": dm.MappedProperty(
                    container=dm.ContainerId("cdf_cdm", "CogniteAsset"),
                    container_property_identifier="parent",
                    type=dm.DirectRelation(),
                    nullable=True,
                    auto_increment=False,
                    immutable=False,
                    source=dm.ViewId("my_space", "Tag", "v1"),
                ),
                "children": dm.MultiReverseDirectRelation(
                    source=dm.ViewId("my_space", "Tag", "v1"),
                    through=dm.PropertyId(
                        source=dm.ViewId("cdf_cdm", "CogniteAsset", "v1"),
                        property="parent",
                    ),
                ),
            },
            description=None,
            is_global=False,
            last_updated_time=0,
            created_time=0,
            used_for="node",
            writable=True,
            filter=None,
        )
    ],
)
