import uuid
from dataclasses import dataclass
from typing import Callable

import pytest

from cognite.pygen.utils.external_id_factories import (
    ExternalIdFactory,
    create_external_id_factory,
    domain_name_factory,
    external_id_generator,
    incremental_factory,
    sha256_factory,
    shorten_string,
    uuid_factory,
)


@dataclass
class FooWrite:
    name: str


@dataclass
class FooBarWrite:
    name: str


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
    "domain_cls, data, shorten_length, expected, short_expected",
    [
        (FooWrite, {}, 7, "foo", "foo"),
        (FooBarWrite, {}, 5, "foobar", "fooba"),
        (FooBarApply, {}, 7, "foobarapply", "foobara"),
    ],
)
def test_domain_name_factory(domain_cls: type, data: dict, shorten_length: int, expected: str, short_expected: str):
    actual = domain_name_factory(domain_cls, data)
    domain_name_factory.shorten_length = shorten_length
    short_actual = domain_name_factory.short(domain_cls, data)

    assert actual == expected
    assert short_actual == short_expected


@pytest.mark.parametrize(
    "domain_cls, data, shorten_length",
    [
        # TODO: Add more test cases with data
        (FooWrite, {}, 7),
        (FooBarWrite, {}, 5),
        (FooBarApply, {}, 3),
    ],
)
def test_uuid_factory(domain_cls: type, data: dict, shorten_length: int):
    actual = uuid_factory(domain_cls, data)
    uuid_factory.shorten_length = shorten_length
    short_actual = uuid_factory.short(domain_cls, data)

    assert uuid.UUID(actual)
    assert len(short_actual) == shorten_length


@pytest.mark.parametrize(
    "data, shorten_length, expected, short_expected",
    [
        ({"name": "foo"}, 7, "c6c9d7497c9a7d180d7c8a5f88b01a5030260cc4bc833467fcc45dcfb1a4a4d1", "c6c9d74"),
        ({"name": "foobar"}, 5, "df323f497c749b1f0d0ce494f848404c4bf33e09262b787c293c5e472155aca9", "df323"),
        ({"name": "foobarapply"}, 3, "fa1f2fc8a7cb05e8f4549d58c7dcb62756f191557a06c1ebcb9044159cbf81d8", "fa1"),
    ],
)
def test_sha256_factory(data: dict, shorten_length: int, expected: str, short_expected: str):
    actual = sha256_factory(FooBarWrite, data)
    sha256_factory.shorten_length = shorten_length
    short_actual = sha256_factory.short(FooBarApply, data)

    assert actual == expected
    assert short_actual == short_expected


@pytest.mark.parametrize(
    "domain_cls, shorten_length, expected, short_expected",
    [
        # TODO: Add more test cases
        (FooWrite, 7, "1", "2"),
        (FooWrite, 7, "3", "4"),
        (FooWrite, 7, "5", "6"),
        (FooWrite, 7, "7", "8"),
        (FooWrite, 7, "9", "10"),
        (FooWrite, 7, "11", "12"),
        (FooWrite, 1, "13", "1"),
        (FooWrite, 1, "15", "1"),
        (FooBarWrite, 7, "1", "2"),
        (FooBarApply, 7, "1", "2"),
    ],
)
def test_incremental_factory(domain_cls: type, shorten_length: int, expected: str, short_expected: str):
    actual = incremental_factory(domain_cls, {})
    incremental_factory.shorten_length = shorten_length
    short_actual = incremental_factory.short(domain_cls, {})

    assert actual == expected
    assert short_actual == short_expected


def test_external_id_generator_default():
    actual = external_id_generator(FooBarWrite, {"external_id": "foobar:123"})

    assert actual.startswith("foobar:")
    assert uuid.UUID(actual.removeprefix("foobar:"))


def test_external_id_generator_override_false():
    actual = external_id_generator(FooBarWrite, {"external_id": "foobar_123"}, override_external_id=False)

    assert actual == "foobar_123"


@pytest.mark.parametrize(
    "domain_cls, data, override_external_id, separator, prefix_function, suffix_function, expected",
    [
        (
            FooBarWrite,
            {"name": "foo"},
            True,
            "_",
            domain_name_factory,
            sha256_factory,
            "foobar_c6c9d7497c9a7d180d7c8a5f88b01a5030260cc4bc833467fcc45dcfb1a4a4d1",
        ),
        (
            FooBarWrite,
            {"name": "foo"},
            True,
            "|",
            ExternalIdFactory(lambda domain_cls, data: f"test{data.get('name', 'no_name')}"),
            sha256_factory,
            "testfoo|c6c9d7497c9a7d180d7c8a5f88b01a5030260cc4bc833467fcc45dcfb1a4a4d1",
        ),
        (
            FooBarWrite,
            {"name": "foo"},
            True,
            "|",
            ExternalIdFactory(lambda domain_cls, data: "test"),
            sha256_factory,
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
    actual = external_id_generator(domain_cls, data, override_external_id, separator, prefix_function, suffix_function)

    assert actual == expected


def test_create_external_id_factory_default():
    factory = create_external_id_factory()

    assert callable(factory)


def test_create_external_id_factory_invalid():
    with pytest.raises(ValueError):
        create_external_id_factory(separator="ab")


def test_create_external_id_factory_custom():
    factory = create_external_id_factory(
        separator="|",
        prefix_ext_id_factory=ExternalIdFactory(factory=lambda domain_cls, data: "test", shorten_length=3),
        suffix_ext_id_factory=ExternalIdFactory(
            factory=lambda domain_cls, data: data.get("name", "no_name"), shorten_length=3
        ),
    )
    actual_1 = factory(FooBarWrite, {"name": "foo"})
    actual_2 = factory(FooBarWrite, {})

    assert callable(factory)
    assert actual_1 == "tes|foo"
    assert actual_2 == "tes|no_"
