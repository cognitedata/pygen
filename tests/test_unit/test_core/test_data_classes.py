from __future__ import annotations

from collections import defaultdict
from itertools import chain
from unittest.mock import MagicMock

import pytest
from cognite.client import data_modeling as dm
from yaml import safe_load

from cognite.pygen._core.data_classes import (
    DataClass,
    EdgeOneToOne,
    Field,
    FilterCondition,
    FilterParameter,
    PrimitiveField,
    PrimitiveListField,
    ViewSpaceExternalId,
)
from cognite.pygen.config import PygenConfig
from cognite.pygen.warnings import (
    ParameterNameCollisionWarning,
    ViewNameCollisionWarning,
    ViewPropertyNameCollisionWarning,
)
from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from movie_domain.client.data_classes import DomainModel, DomainModelApply
else:
    from movie_domain_pydantic_v1.client.data_classes import DomainModel, DomainModelApply


def load_field_test_cases():
    raw_data = {
        "container": {"space": "power-ops", "externalId": "BenchmarkProcess"},
        "containerPropertyIdentifier": "runEvents",
        "type": {"list": True, "collation": "ucs_basic", "type": "text"},
        "nullable": True,
        "autoIncrement": False,
        "source": None,
        "defaultValue": None,
        "name": "runEvents",
        "description": None,
    }
    mapped = dm.MappedProperty.load(raw_data)
    yield pytest.param(
        mapped,
        PrimitiveListField(
            name="run_events",
            prop_name="runEvents",
            pydantic_field="Field",
            type_="str",
            is_nullable=True,
            prop=mapped,
            doc_name="run event",
        ),
        {},
        'Optional[list[str]] = Field(None, alias="runEvents")',
        'Optional[list[str]] = Field(None, alias="runEvents")',
        id="PrimitiveListField that require alias.",
    )
    raw_data = {
        "container": {"space": "cogShop", "external_id": "Scenario"},
        "container_property_identifier": "modelTemplate",
        "type": {
            "container": None,
            "type": "direct",
            "source": {"space": "cogShop", "external_id": "ModelTemplate", "version": "8ae35635bb3f8a"},
        },
        "nullable": True,
        "auto_increment": False,
        "default_value": None,
        "name": "modelTemplate",
        "description": None,
    }
    mapped = dm.MappedProperty.load(raw_data)
    data_class = MagicMock(spec=DataClass)
    data_class.write_name = "ModelTemplateApply"

    yield pytest.param(
        mapped,
        EdgeOneToOne(
            name="model_template",
            prop_name="modelTemplate",
            pydantic_field="Field",
            data_class=data_class,
            prop=mapped,
            doc_name="model template",
        ),
        {ViewSpaceExternalId("cogShop", "ModelTemplate"): data_class},
        'Optional[str] = Field(None, alias="modelTemplate")',
        'Union[ModelTemplateApply, str, None] = Field(None, repr=False, alias="modelTemplate")',
        id="EdgeField that require alias.",
    )
    raw_data = """
    auto_increment: false
    container:
      external_id: Market
      space: market
    container_property_identifier: name
    default_value: null
    description: null
    name: name
    nullable: true
    source: null
    type:
      collation: ucs_basic
      list: false
      type: text
    """
    mapped = dm.MappedProperty.load(safe_load(raw_data))
    yield pytest.param(
        mapped,
        PrimitiveField(
            name="name",
            prop_name="name",
            pydantic_field="Field",
            type_="str",
            is_nullable=True,
            prop=mapped,
            default=None,
            doc_name="name",
        ),
        {},
        "Optional[str] = None",
        "Optional[str] = None",
        id="Field that does not require alias.",
    )


@pytest.mark.parametrize(
    "property_, expected, data_class_by_id, read_type_hint, write_type_hint", load_field_test_cases()
)
def test_load_field(
    property_: dm.MappedProperty | dm.ConnectionDefinition,
    expected: Field,
    data_class_by_id: dict[ViewSpaceExternalId, DataClass],
    read_type_hint: str,
    write_type_hint: str,
    pygen_config: PygenConfig,
) -> None:
    # Act
    actual = Field.from_property(
        property_.name,
        property_,
        data_class_by_id,
        pygen_config.naming.field,
        view_name="dummy",
        view_id=dm.ViewId("a", "b", "c"),
    )

    # Assert
    assert actual == expected
    assert actual.as_write_type_hint() == write_type_hint
    assert actual.as_read_type_hint() == read_type_hint


def load_data_classes_test_cases():
    raw_data = {
        "space": "power-ops",
        "externalId": "Series",
        "name": "Series",
        "version": "59d189398e78be",
        "writable": True,
        "usedFor": "node",
        "isGlobal": False,
        "properties": {
            "timeIntervalStart": {
                "container": {"space": "power-ops", "externalId": "Series"},
                "containerPropertyIdentifier": "timeIntervalStart",
                "type": {"list": False, "type": "timestamp"},
                "nullable": True,
                "autoIncrement": False,
                "source": None,
                "defaultValue": None,
                "name": "timeIntervalStart",
                "description": None,
            },
            "timeIntervalEnd": {
                "container": {"space": "power-ops", "externalId": "Series"},
                "containerPropertyIdentifier": "timeIntervalEnd",
                "type": {"list": False, "type": "timestamp"},
                "nullable": True,
                "autoIncrement": False,
                "source": None,
                "defaultValue": None,
                "name": "timeIntervalEnd",
                "description": None,
            },
            "resolution": {
                "container": {"space": "power-ops", "externalId": "Series"},
                "containerPropertyIdentifier": "resolution",
                "type": {
                    "container": None,
                    "type": "direct",
                    "source": {"space": "power-ops", "externalId": "Duration", "version": "7433a3f6ac2be0"},
                },
                "nullable": True,
                "autoIncrement": False,
                "defaultValue": None,
                "name": "resolution",
                "description": None,
            },
            "points": {
                "type": {"space": "power-ops", "externalId": "Series.points"},
                "source": {"space": "power-ops", "externalId": "Point", "version": "791cb15b0ae9e1", "type": "view"},
                "name": "points",
                "description": None,
                "edgeSource": None,
                "direction": "outwards",
            },
        },
        "lastUpdatedTime": 1695295084756,
        "createdTime": 1695295084756,
    }
    view = dm.View.load(raw_data)
    yield pytest.param(
        view,
        DataClass(
            read_name="Series",
            write_name="SeriesApply",
            write_list_name="SeriesApplyList",
            read_list_name="SeriesList",
            doc_name="series",
            doc_list_name="series",
            view_id=ViewSpaceExternalId(view.space, view.external_id),
            view_version=view.version,
            variable="series",
            variable_list="series_list",
            view_name="Series",
            file_name="_series",
            fields=[],
        ),
        id="DataClass variable and variable_list the same.",
    )


@pytest.mark.parametrize("view, expected", load_data_classes_test_cases())
def test_load_data_class(view: dm.View, expected: DataClass, pygen_config: PygenConfig) -> None:
    # Act
    actual = DataClass.from_view(view, pygen_config.naming.data_class)

    # Assert
    assert actual == expected


def test_data_class_is_time(pygen_config: PygenConfig) -> None:
    # Arrange
    raw_data = {
        "space": "power-ops",
        "externalId": "PriceArea",
        "name": "PriceArea",
        "version": "6849ae787cd368",
        "writable": True,
        "usedFor": "node",
        "isGlobal": False,
        "properties": {
            "name": {
                "container": {"space": "power-ops", "externalId": "PriceArea"},
                "containerPropertyIdentifier": "name",
                "type": {"list": False, "collation": "ucs_basic", "type": "text"},
                "nullable": True,
                "autoIncrement": False,
                "source": None,
                "defaultValue": None,
                "name": "name",
                "description": None,
            },
            "description": {
                "container": {"space": "power-ops", "externalId": "PriceArea"},
                "containerPropertyIdentifier": "description",
                "type": {"list": False, "collation": "ucs_basic", "type": "text"},
                "nullable": True,
                "autoIncrement": False,
                "source": None,
                "defaultValue": None,
                "name": "description",
                "description": None,
            },
            "dayAheadPrice": {
                "container": {"space": "power-ops", "externalId": "PriceArea"},
                "containerPropertyIdentifier": "dayAheadPrice",
                "type": {"list": False, "type": "timeseries"},
                "nullable": True,
                "autoIncrement": False,
                "source": None,
                "defaultValue": None,
                "name": "dayAheadPrice",
                "description": None,
            },
            "plants": {
                "type": {"space": "power-ops", "externalId": "PriceArea.plants"},
                "source": {"space": "power-ops", "externalId": "Plant", "version": "836dcb3f5da1df", "type": "view"},
                "name": "plants",
                "description": None,
                "edgeSource": None,
                "direction": "outwards",
            },
            "watercourses": {
                "type": {"space": "power-ops", "externalId": "PriceArea.watercourses"},
                "source": {
                    "space": "power-ops",
                    "externalId": "Watercourse",
                    "version": "96f5170f35ef70",
                    "type": "view",
                },
                "name": "watercourses",
                "description": None,
                "edgeSource": None,
                "direction": "outwards",
            },
        },
        "lastUpdatedTime": 1692020117686,
        "createdTime": 1692020117686,
    }
    view = dm.View.load(raw_data)

    # Act
    data_class = DataClass.from_view(view, pygen_config.naming.data_class)
    data_class.update_fields(view.properties, defaultdict(lambda: MagicMock(spec=DataClass)), pygen_config.naming.field)

    # Assert
    assert data_class.has_single_timeseries_fields is True


@pytest.mark.parametrize(
    "filter_condition, expected_args",
    [
        (
            FilterCondition(
                dm.filters.Range,
                prop_name="end_time",
                keyword_arguments={
                    "gte": FilterParameter("min_end_time", "datetime.datetime", description="Dummy."),
                    "lte": FilterParameter("max_end_time", "datetime.datetime", description="Dummy."),
                },
            ),
            'view_id.as_property_ref("end_time"), '
            'gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None, '
            'lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None',
        )
    ],
)
def test_filter_condition(filter_condition: FilterCondition, expected_args: str) -> None:
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
    ]
    + [(name, f"{name}_") for name in chain(dir(DomainModel), dir(DomainModelApply)) if not name.startswith("_")],
)
def test_field_from_property_expect_warning(name: str, expected_name, pygen_config: PygenConfig) -> None:
    # Arrange
    prop = dm.MappedProperty(dm.ContainerId("dummy", "dummy"), name, dm.Text(), True, True, name=name)

    # Act
    with pytest.warns(ViewPropertyNameCollisionWarning):
        actual = Field.from_property(name, prop, {}, pygen_config.naming.field, "dummy", dm.ViewId("a", "b", "c"))

    # Assert
    assert actual.name == expected_name


@pytest.mark.parametrize(
    "name, expected_name",
    [
        ("DomainModel", "DomainModel_"),
        ("DomainModelList", "DomainModelList_"),
        ("DomainModelApply", "DomainModelApply_"),
    ],
)
def test_data_class_from_view_expected_warning(name: str, expected_name: str, pygen_config: PygenConfig) -> None:
    # Arrange
    view = dm.View(
        "dummy",
        name,
        "dummy",
        {},
        last_updated_time=1,
        created_time=1,
    )

    # Act
    with pytest.warns(ViewNameCollisionWarning):
        actual = DataClass.from_view(view, pygen_config.naming.data_class)
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
        "dummy",
        name,
        "dummy",
        {},
        last_updated_time=1,
        created_time=1,
    )

    # Act
    with pytest.warns(ViewNameCollisionWarning):
        actual = DataClass.from_view(view, pygen_config.naming.data_class)
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
