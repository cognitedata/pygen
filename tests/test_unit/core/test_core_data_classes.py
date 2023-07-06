from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Optional

from pydantic import Field

from cognite.pygen._core.static._core_data_classes import DomainModel


class Foo(DomainModel):
    space: ClassVar[str] = "FooSpace"
    bar: Optional[Bar] = Field(None, repr=False)


class Bar(DomainModel):
    space: ClassVar[str] = "BarSpace"
    foo: Optional[Foo] = Field(None, repr=False)


Foo.model_rebuild()
Bar.model_rebuild()


class TestDomainModel:
    def test_repr(self):
        # Arrange
        foo = Foo(
            external_id="foo", version=1, created_time=datetime(2023, 1, 1), last_updated_time=datetime(2024, 1, 1)
        )
        bar = Bar(
            external_id="bar", version=1, created_time=datetime(2023, 1, 1), last_updated_time=datetime(2024, 1, 1)
        )
        foo.bar = bar
        bar.foo = foo

        # Act
        foo_repr = repr(foo)
        bar_repr = repr(bar)

        # Assert
        assert (
            foo_repr == "Foo(external_id='foo', version=1, last_updated_time=datetime.datetime(2024, 1, 1, 0, 0), "
            "created_time=datetime.datetime(2023, 1, 1, 0, 0), deleted_time=None)"
        )
        assert (
            bar_repr == "Bar(external_id='bar', version=1, last_updated_time=datetime.datetime(2024, 1, 1, 0, 0), "
            "created_time=datetime.datetime(2023, 1, 1, 0, 0), deleted_time=None)"
        )
