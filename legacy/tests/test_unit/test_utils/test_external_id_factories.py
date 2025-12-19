import uuid as uuid_
from collections.abc import Callable
from dataclasses import dataclass

import pytest
from wind_turbine.data_classes import DomainModelWrite, RotorWrite, WindTurbineWrite

from cognite.pygen.utils.external_id_factories import (
    ExternalIdFactory,
    domain_name,
    incremental_id,
    sha256,
    shorten_string,
    uuid,
)


@dataclass
class FooBarApply:
    name: str


@pytest.mark.parametrize(
    "input_str, length, expected",
    [
        ("fooBar", 0, ""),
        ("fooBar", 1, "f"),
        ("fooBar", 2, "fo"),
        ("fooBar", 3, "foo"),
        ("fooBar", 4, "fooB"),
        ("fooBar", 5, "fooBa"),
        ("fooBar", 6, "fooBar"),
        ("fooBar", 7, "fooBar"),
        ("12", 1, "1"),
        ("14", 1, "1"),
    ],
)
def test_shorten_string(input_str: str, length: int, expected: str):
    actual = shorten_string(input_str, length)

    assert actual == expected


@pytest.mark.parametrize(
    "domain_cls, data, expected, short_expected",
    [
        (WindTurbineWrite, {}, "windturbine", "windtur"),
        (RotorWrite, {}, "rotor", "rotor"),
        (FooBarApply, {}, "foobarapply", "foobara"),
    ],
)
def test_domain_name_factory(domain_cls: type, data: dict, expected: str, short_expected: str):
    actual = domain_name(domain_cls, data)
    func = ExternalIdFactory.domain_name_factory()
    actual_factory = func(domain_cls, data)
    short_actual = func.short(domain_cls, data)

    assert actual == expected
    assert actual_factory == expected
    assert short_actual == short_expected


@pytest.mark.parametrize(
    "domain_cls, data",
    [
        (WindTurbineWrite, {}),
        (RotorWrite, {}),
        (FooBarApply, {}),
    ],
)
def test_uuid_factory(domain_cls: type, data: dict):
    actual = uuid(domain_cls, data)
    func = ExternalIdFactory.uuid_factory()
    actual_factory = func(domain_cls, data)
    short_actual = func.short(domain_cls, data)

    assert uuid_.UUID(actual)
    assert uuid_.UUID(actual_factory)
    assert actual != actual_factory
    assert len(short_actual) == 7


@pytest.mark.parametrize(
    "data, expected, short_expected",
    [
        ({"name": "foo"}, "c6c9d7497c9a7d180d7c8a5f88b01a5030260cc4bc833467fcc45dcfb1a4a4d1", "c6c9d74"),
        ({"name": "foobar"}, "df323f497c749b1f0d0ce494f848404c4bf33e09262b787c293c5e472155aca9", "df323f4"),
        ({"name": "foobarapply"}, "fa1f2fc8a7cb05e8f4549d58c7dcb62756f191557a06c1ebcb9044159cbf81d8", "fa1f2fc"),
    ],
)
def test_sha256_factory(data: dict, expected: str, short_expected: str):
    actual = sha256(RotorWrite, data)
    func = ExternalIdFactory.sha256_factory()
    actual_factory = func(RotorWrite, data)
    short_actual = func.short(FooBarApply, data)

    assert actual == expected
    assert actual_factory == expected
    assert short_actual == short_expected


@pytest.mark.skip("This depends on the order of the test executions. Needs rewrite.")
@pytest.mark.parametrize(
    "domain_cls, expected, expected_factory, short_expected",
    [
        (WindTurbineWrite, "1", "2", "3"),
        (WindTurbineWrite, "4", "5", "6"),
        (RotorWrite, "1", "2", "3"),
        (FooBarApply, "1", "2", "3"),
    ],
)
def test_incremental_factory(domain_cls: type, expected: str, expected_factory: str, short_expected: str):
    actual = incremental_id(domain_cls, {})
    func = ExternalIdFactory.incremental_factory()
    actual_factory = func(domain_cls, {})
    short_actual = func.short(domain_cls, {})

    assert actual == expected
    assert actual_factory == expected_factory
    assert short_actual == short_expected


def test_external_id_generator_default():
    actual = ExternalIdFactory.external_id_generator(RotorWrite, {"external_id": "rotor:123"})

    assert actual.startswith("rotor:")
    assert uuid_.UUID(actual.removeprefix("rotor:"))


def test_external_id_generator_override_false():
    actual = ExternalIdFactory.external_id_generator(
        RotorWrite, {"external_id": "foobar_123"}, override_external_id=False
    )

    assert actual == "foobar_123"


@pytest.mark.parametrize(
    "domain_cls, data, override_external_id, separator, prefix_function, suffix_function, expected",
    [
        (
            RotorWrite,
            {"name": "foo"},
            True,
            "_",
            ExternalIdFactory.domain_name_factory(),
            ExternalIdFactory.sha256_factory(),
            "rotor_c6c9d7497c9a7d180d7c8a5f88b01a5030260cc4bc833467fcc45dcfb1a4a4d1",
        ),
        (
            RotorWrite,
            {"name": "foo"},
            True,
            "|",
            ExternalIdFactory(lambda domain_cls, data: f"test{data.get('name', 'no_name')}"),
            ExternalIdFactory.sha256_factory(),
            "testfoo|c6c9d7497c9a7d180d7c8a5f88b01a5030260cc4bc833467fcc45dcfb1a4a4d1",
        ),
        (
            WindTurbineWrite,
            {"name": "foo"},
            True,
            "|",
            ExternalIdFactory(lambda domain_cls, data: "test"),
            ExternalIdFactory.sha256_factory(),
            "test|c6c9d7497c9a7d180d7c8a5f88b01a5030260cc4bc833467fcc45dcfb1a4a4d1",
        ),
    ],
)
def test_external_id_generator(
    domain_cls: type,
    data: dict,
    override_external_id: bool,
    separator: str,
    prefix_function: Callable[[type, dict], str],
    suffix_function: Callable[[type, dict], str],
    expected: str,
):
    actual = ExternalIdFactory.external_id_generator(
        domain_cls, data, override_external_id, separator, prefix_function, suffix_function
    )

    assert actual == expected


def test_create_external_id_factory_default():
    factory = ExternalIdFactory.create_external_id_factory()

    actual_1 = factory(RotorWrite, {"name": "foo"})
    actual_2 = factory(RotorWrite, {})

    expected_prefix = "rotor:"

    assert callable(factory)
    assert actual_1.startswith(expected_prefix)
    assert uuid_.UUID(actual_1.removeprefix(expected_prefix))
    assert actual_2.startswith(expected_prefix)
    assert uuid_.UUID(actual_2.removeprefix(expected_prefix))


def test_create_external_id_factory_invalid():
    with pytest.raises(ValueError):
        ExternalIdFactory.create_external_id_factory(separator="ab")


def test_create_external_id_factory_short():
    factory = ExternalIdFactory.create_external_id_factory(
        separator="|",
        prefix_ext_id_factory=ExternalIdFactory.domain_name_factory().short,
        suffix_ext_id_factory=ExternalIdFactory.uuid_factory().short,
    )
    actual_1 = factory(WindTurbineWrite, {"name": "foo"})
    actual_2 = factory(WindTurbineWrite, {})

    expected_prefix = "windtur|"

    assert callable(factory)
    assert actual_1.startswith(expected_prefix)
    assert len(actual_1.removeprefix(expected_prefix)) == 7
    assert actual_2.startswith(expected_prefix)
    assert len(actual_2.removeprefix(expected_prefix)) == 7
    assert actual_1 != actual_2


def test_create_external_id_factory_custom():
    factory = ExternalIdFactory.create_external_id_factory(
        separator="|",
        prefix_ext_id_factory=ExternalIdFactory(factory=lambda domain_cls, data: "test", shorten_length=3),
        suffix_ext_id_factory=ExternalIdFactory(
            factory=lambda domain_cls, data: data.get("name", "no_name"), shorten_length=3
        ),
    )
    actual_1 = factory(WindTurbineWrite, {"name": "foo"})
    actual_2 = factory(WindTurbineWrite, {})

    assert callable(factory)
    assert actual_1 == "tes|foo"
    assert actual_2 == "tes|no_"


@pytest.mark.skip("There is something strange with the uuid_.UUID assert")
def test_doc_example():
    fallback_factory = ExternalIdFactory.uuid_factory()

    def custom_factory_by_domain_class(domain_cls: type, data: dict) -> str:
        if domain_cls is WindTurbineWrite and "name" in data:
            return data["name"]
        elif domain_cls is RotorWrite and "name" in data:
            return data["name"]
        else:
            # Fallback to uuid factory
            return fallback_factory(domain_cls, data)

    # Finally, we can configure the new factory on the DomainModelWrite class a few different ways:

    # Using the custom factory directly:
    DomainModelWrite.external_id_factory = ExternalIdFactory(custom_factory_by_domain_class)
    # Example input and output:
    turbine_with_name = WindTurbineWrite(name="Oslo 1")
    assert turbine_with_name.external_id == "Oslo 1"
    rotor_no_name = RotorWrite(rotor_speed_controller="time_series_external_id")
    assert uuid_.UUID(rotor_no_name.external_id)
    turbine_with_external_id = WindTurbineWrite(external_id="custom_external_id")
    assert uuid_.UUID(turbine_with_external_id.external_id)

    # Using the create_external_id_factory function to have a prefix and suffix combination:
    DomainModelWrite.external_id_factory = ExternalIdFactory.create_external_id_factory(
        override_external_id=False,
        separator="_",
        prefix_ext_id_factory=ExternalIdFactory.domain_name_factory(),
        suffix_ext_id_factory=ExternalIdFactory(custom_factory_by_domain_class),
    )
    # Example input and output:
    turbine_with_name = WindTurbineWrite(name="Oslo 1")
    assert turbine_with_name.external_id == "wind_turbine_Oslo 1"
    rotor_no_name = RotorWrite(rotor_speed_controller="time_series_external_id")
    assert rotor_no_name.external_id.startswith("rotor_")
    assert uuid_.UUID(rotor_no_name.external_id.removeprefix("rotor_"))
    turbine_with_external_id = WindTurbineWrite(external_id="custom_external_id")
    assert turbine_with_external_id.external_id == "custom_external_id"


def test_domain_name_for_class_name_ending_with_write():
    class _FooBarWrite:
        """Class not inheriting from DomainModelWrite with name ending with Write"""

        pass

    assert domain_name(_FooBarWrite, {}) == "_foobarwrite"
    assert domain_name(WindTurbineWrite, {}) == "windturbine"
