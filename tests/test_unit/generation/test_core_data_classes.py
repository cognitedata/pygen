from __future__ import annotations

import datetime
from typing import ClassVar, Optional

from pydantic import Field

from cognite.pygen._core.static._core_data_classes import DomainModel


class TestDomainModel:
    def test_repr(self):
        # Arrange
        class Foo(DomainModel):
            space: ClassVar[str] = "FooSpace"
            bar: Optional[Bar] = Field(None, repr=False)

        class Bar(DomainModel):
            space: ClassVar[str] = "BarSpace"
            foo: Optional[Foo] = Field(None, repr=False)

        try:
            Foo.model_rebuild()
            Bar.model_rebuild()
        except AttributeError as e:
            if "has no attribute 'model_rebuild" in str(e):
                # is pydantic v1
                Foo.update_forward_refs(Bar=Bar)
                Bar.update_forward_refs(Foo=Foo)
            else:
                raise e

        foo = Foo(
            external_id="foo",
            version=1,
            created_time=datetime.datetime(2023, 1, 1),
            last_updated_time=datetime.datetime(2024, 1, 1),
        )
        bar = Bar(
            external_id="bar",
            version=1,
            created_time=datetime.datetime(2023, 1, 1),
            last_updated_time=datetime.datetime(2024, 1, 1),
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
