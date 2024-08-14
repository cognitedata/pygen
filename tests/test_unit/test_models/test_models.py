from __future__ import annotations

from itertools import chain
from unittest.mock import MagicMock

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import ViewProperty

from cognite.pygen._core.models import (
    EndNodeField,
    Field,
    FilterImplementation,
    FilterParameter,
    NodeDataClass,
    OneToManyConnectionField,
    OneToOneConnectionField,
    PrimitiveField,
    PrimitiveListField,
)
from cognite.pygen._core.models.fields import EdgeClasses
from cognite.pygen.config import PygenConfig
from cognite.pygen.warnings import (
    ParameterNameCollisionWarning,
    ViewNameCollisionWarning,
    ViewPropertyNameCollisionWarning,
)
from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni.data_classes import DomainModel, DomainModelWrite
else:
    from omni_pydantic_v1.data_classes import DomainModel, DomainModelWrite


@pytest.mark.parametrize(
    "filter_condition, expected_args",
    [
        (
            FilterImplementation(
                dm.filters.Range,
                prop_name="end_time",
                keyword_arguments={
                    "gte": FilterParameter("min_end_time", "datetime.datetime", description="Dummy."),
                    "lte": FilterParameter("max_end_time", "datetime.datetime", description="Dummy."),
                },
                is_edge_class=False,
            ),
            'view_id.as_property_ref("end_time"), '
            'gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None, '
            'lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None',
        )
    ],
)
def test_filter_condition(filter_condition: FilterImplementation, expected_args: str) -> None:
    # Act
    actual = filter_condition.arguments

    # Assert
    assert actual == expected_args


@pytest.mark.parametrize(
    "name, expected_name",
    [
        ("property", "property_"),
        ("version", "version_"),
        ("yield", "yield_"),
        ("len", "len_"),
        ("def", "def_"),
        *{
            (name, f"{name.casefold()}_")
            for name in chain(dir(DomainModel), dir(DomainModelWrite))
            if not name.startswith("_")
        },
    ],
)
def test_field_from_property_expect_warning(name: str, expected_name, pygen_config: PygenConfig) -> None:
    # Arrange
    prop = dm.MappedProperty(
        container=dm.ContainerId("dummy", "dummy"),
        container_property_identifier=name,
        type=dm.Text(),
        nullable=True,
        immutable=False,
        auto_increment=False,
        name=name,
    )

    # Act
    with pytest.warns(ViewPropertyNameCollisionWarning):
        actual = Field.from_property(name, prop, {}, {}, pygen_config, dm.ViewId("a", "b", "c"), pydantic_field="Field")

    # Assert
    assert actual.name == expected_name


@pytest.mark.parametrize(
    "name, expected_name",
    [
        ("DomainModel", "DomainModel_"),
        ("DomainModelList", "DomainModelList_"),
        ("DomainModelWrite", "DomainModelWrite_"),
    ],
)
def test_data_class_from_view_expected_warning(name: str, expected_name: str, pygen_config: PygenConfig) -> None:
    # Arrange
    view = dm.View(
        space="dummy",
        external_id=name,
        version="dummy",
        properties={},
        name=name,
        last_updated_time=1,
        created_time=1,
        is_global=False,
        description="missing",
        used_for="node",
        writable=True,
        filter=None,
        implements=None,
    )

    # Act
    with pytest.warns(ViewNameCollisionWarning):
        actual = NodeDataClass.from_view(view, NodeDataClass.to_base_name(view), "node", pygen_config.naming.data_class)
    # Assert
    assert actual.read_name == expected_name


@pytest.mark.parametrize(
    "name, expected_name",
    [
        ("Core", "_core_"),
    ],
)
def test_data_class_from_view_expected_warning_file_name(
    name: str, expected_name: str, pygen_config: PygenConfig
) -> None:
    # Arrange
    view = dm.View(
        space="dummy",
        external_id=name,
        version="dummy",
        properties={},
        name=name,
        last_updated_time=1,
        created_time=1,
        is_global=False,
        description="missing",
        used_for="node",
        writable=True,
        filter=None,
        implements=None,
    )

    # Act
    with pytest.warns(ViewNameCollisionWarning):
        actual = NodeDataClass.from_view(view, NodeDataClass.to_base_name(view), "node", pygen_config.naming.data_class)
    # Assert
    assert actual.file_name == expected_name


@pytest.mark.parametrize(
    "name, expected_name",
    [
        ("interval", "interval_"),
        ("limit", "limit_"),
        ("filter", "filter_"),
        ("replace", "replace_"),
        ("retrieve_edges", "retrieve_edges_"),
        ("property", "property_"),
    ],
)
def test_filter_parameter_expected_warning(name: str, expected_name: str, pygen_config: PygenConfig) -> None:
    # Act
    with pytest.warns(ParameterNameCollisionWarning):
        actual = FilterParameter(name, "str", "dummy")

    # Assert
    assert actual.name == expected_name


def create_fields_test_cases():
    prop = {
        "container": {"space": "IntegrationTestsImmutable", "externalId": "Person"},
        "containerPropertyIdentifier": "name",
        "type": {"list": False, "collation": "ucs_basic", "type": "text"},
        "nullable": False,
        "autoIncrement": False,
        "source": None,
        "defaultValue": None,
        "name": "name",
        "description": None,
        "immutable": False,
    }
    prop = ViewProperty.load(prop)
    #
    yield pytest.param(
        "name",
        prop,
        {},
        "Person",
        PrimitiveField(
            name="name",
            prop_name="name",
            doc_name="name",
            type_=dm.Text(),
            description=None,
            is_nullable=False,
            default=None,
            pydantic_field="Field",
        ),
        "str",
        "str",
        id="String property",
    )
    prop = {
        "connectionType": "multi_edge_connection",
        "type": {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
        "source": {"space": "IntegrationTestsImmutable", "externalId": "Role", "version": "2", "type": "view"},
        "name": "roles",
        "description": None,
        "edgeSource": None,
        "direction": "outwards",
    }
    prop = ViewProperty.load(prop)
    data_class = NodeDataClass(
        read_name="Role",
        write_name="RoleApply",
        graphql_name="RoleGraphQL",
        read_list_name="RoleList",
        write_list_name="RoleListApply",
        doc_name="role",
        doc_list_name="roles",
        variable="role",
        description="No description",
        variable_list="roles",
        file_name="_roles",
        view_id=dm.ViewId("IntegrationTestsImmutable", "Role", "2"),
        fields=[],
        node_type=None,
        is_interface=True,
        is_writable=True,
        implements=[],
        initialization=set(),
        has_edge_class=False,
    )
    data_class_by_view_id = {dm.ViewId("IntegrationTestsImmutable", "Role", "2"): data_class}
    yield pytest.param(
        "roles",
        prop,
        data_class_by_view_id,
        "Person",
        OneToManyConnectionField(
            name="roles",
            prop_name="roles",
            doc_name="role",
            description=None,
            end_classes=[data_class],
            variable="role",
            pydantic_field="Field",
            edge_type=dm.DirectRelationReference("IntegrationTestsImmutable", "Person.roles"),
            edge_direction="outwards",
            use_node_reference_in_type_hint=True,
            through=None,
            destination_class=None,
        ),
        "Optional[list[Union[Role, str, dm.NodeId]]] = Field(default=None, repr=False)",
        "Optional[list[Union[RoleApply, str, dm.NodeId]]] = Field(default=None, repr=False)",
        id="List of edges",
    )
    prop = {
        "container": {"space": "IntegrationTestsImmutable", "externalId": "Command_Config"},
        "containerPropertyIdentifier": "configs",
        "type": {"list": True, "collation": "ucs_basic", "type": "text"},
        "nullable": False,
        "autoIncrement": False,
        "immutable": False,
        "source": None,
        "defaultValue": None,
        "name": "configs",
        "description": None,
    }
    prop = ViewProperty.load(prop)
    yield pytest.param(
        "configs",
        prop,
        {},
        "Command_Config",
        PrimitiveListField(
            name="configs",
            prop_name="configs",
            description=None,
            type_=dm.Text(is_list=True),
            is_nullable=False,
            pydantic_field="Field",
            doc_name="config",
            variable="config",
        ),
        "list[str]",
        "list[str]",
        id="List of strings",
    )

    prop = {
        "container": {"space": "IntegrationTestsImmutable", "externalId": "Role"},
        "containerPropertyIdentifier": "person",
        "type": {
            "container": None,
            "type": "direct",
            "list": False,
            "source": {"space": "IntegrationTestsImmutable", "externalId": "Person", "version": "2"},
        },
        "nullable": True,
        "autoIncrement": False,
        "immutable": False,
        "defaultValue": None,
        "name": "person",
        "description": None,
    }
    data_class = NodeDataClass(
        read_name="Person",
        write_name="PersonApply",
        graphql_name="PersonGraphQL",
        read_list_name="PersonList",
        write_list_name="PersonListApply",
        doc_name="person",
        doc_list_name="persons",
        description="No description",
        variable="person",
        file_name="_persons",
        view_id=dm.ViewId("IntegrationTestsImmutable", "Person", "2"),
        variable_list="persons",
        fields=[],
        node_type=None,
        is_interface=False,
        is_writable=True,
        implements=[],
        initialization=set(),
        has_edge_class=False,
    )
    data_class_by_view_id = {dm.ViewId("IntegrationTestsImmutable", "Person", "2"): data_class}

    prop = ViewProperty.load(prop)
    yield pytest.param(
        "person",
        prop,
        data_class_by_view_id,
        "Person",
        OneToOneConnectionField(
            name="person",
            prop_name="person",
            doc_name="person",
            end_classes=[data_class],
            pydantic_field="Field",
            description=None,
            use_node_reference_in_type_hint=True,
            edge_type=None,
            edge_direction="outwards",
            through=None,
            destination_class=None,
        ),
        "Union[Person, str, dm.NodeId, None] = Field(default=None, repr=False)",
        "Union[PersonApply, str, dm.NodeId, None] = Field(default=None, repr=False)",
        id="Edge to another view",
    )

    prop = {
        "container": {"space": "IntegrationTestsImmutable", "externalId": "Role"},
        "containerPropertyIdentifier": "wonOscar",
        "type": {"list": False, "type": "boolean"},
        "nullable": True,
        "autoIncrement": False,
        "source": None,
        "defaultValue": None,
        "name": "wonOscar",
        "description": None,
        "immutable": False,
    }
    prop = ViewProperty.load(prop)

    yield pytest.param(
        "wonOscar",
        prop,
        {},
        "Person",
        PrimitiveField(
            name="won_oscar",
            prop_name="wonOscar",
            doc_name="won oscar",
            is_nullable=True,
            default=None,
            type_=dm.Boolean(),
            pydantic_field="Field",
            description=None,
        ),
        'Optional[bool] = Field(None, alias="wonOscar")',
        'Optional[bool] = Field(None, alias="wonOscar")',
        id="Boolean property with pascal name",
    )


@pytest.mark.parametrize(
    "prop_name, property_, data_class_by_view_id, view_name, expected, "
    "expected_read_type_hint, expected_write_type_hint",
    list(create_fields_test_cases()),
)
def test_fields_from_property(
    prop_name: str,
    property_: dm.MappedProperty | dm.ConnectionDefinition,
    data_class_by_view_id: dict[dm.ViewId, NodeDataClass],
    view_name: str,
    expected: Field,
    expected_read_type_hint: str,
    expected_write_type_hint: str,
    pygen_config: PygenConfig,
):
    # Act
    actual = Field.from_property(
        prop_name, property_, data_class_by_view_id, {}, pygen_config, dm.ViewId("a", "b", "c"), pydantic_field="Field"
    )

    # Assert
    assert actual == expected
    assert actual.as_read_type_hint() == expected_read_type_hint
    assert actual.as_write_type_hint() == expected_write_type_hint


def field_type_hints_test_cases():
    site_apply_edge = MagicMock(spec=EdgeClasses)
    site_apply1 = MagicMock(spec=NodeDataClass)
    site_apply1.read_name = "Site"
    site_apply1.write_name = "SiteApply"
    site_apply1.is_writable = True
    site_apply_edge.end_class = site_apply1

    site_apply_edge2 = MagicMock(spec=EdgeClasses)
    site_apply2 = MagicMock(spec=NodeDataClass)
    site_apply2.read_name = "Site"
    site_apply2.write_name = "SiteApply"
    site_apply2.is_writable = True
    site_apply_edge2.end_class = site_apply2

    field = EndNodeField(
        name="end_node",
        doc_name="end node",
        prop_name="end_node",
        description=None,
        pydantic_field="Field",
        edge_classes=[site_apply_edge, site_apply_edge2],
    )
    yield pytest.param(
        field,
        "Union[Site, str, dm.NodeId]",
        "Union[SiteApply, str, dm.NodeId]",
        id="EdgeOneToEndNode",
    )


@pytest.mark.parametrize("field,expected_read_hint, expected_write_hint", list(field_type_hints_test_cases()))
def test_fields_type_hints(field: Field, expected_read_hint: str, expected_write_hint: str) -> None:
    assert field.as_write_type_hint() == expected_write_hint
    assert field.as_read_type_hint() == expected_read_hint
