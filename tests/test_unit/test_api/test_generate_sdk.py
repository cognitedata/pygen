import importlib
import sys
from contextlib import contextmanager
from pathlib import Path

from cognite.client import data_modeling as dm

from cognite.pygen import generate_sdk


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
            output_dir=tmp_path,
            overwrite=True,
            client_name=client_name,
            default_instance_space="field_space",
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
            output_dir=tmp_path,
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
            output_dir=tmp_path,
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
            output_dir=tmp_path,
            overwrite=True,
            client_name=client_name,
            default_instance_space="reverse_direct_relation_space",
        )

        with append_to_sys_path(str(tmp_path)):
            module = vars(importlib.import_module(top_level_package))
            assert client_name in module


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
            },
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
                        source=dm.ViewId("reverse_direct_relation_space", "ReverseDirectRelation", "1"),
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
