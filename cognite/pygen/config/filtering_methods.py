from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field

from cognite.client import data_modeling as dm


@dataclass
class Filtering:
    """The type of filters to use for each property type.

    When pygen generates, for example, a list or timeseries method, it uses the type of the property to determine which
    filters to implement. For example, if you have two properties, `year` of type Int32 and `name` of type Text,
    and you generate a list method with the default options, you will get the following filters:

    ```python
    class MyAPIClass:
        ...
        def list(self,
            min_year: int | None = None,
            max_year: int | None = None,
            name: str | list[str] | None = None,
            name_prefix: str | None = None,
            external_id_prefix: str | None = None,
        ):
            ...

    ```

    !!! warning "Not supported properties"
        Currently primitive type properties and one-to-one edges are supported. If you have a list of
        primitive types, e.g., list of strings, it will not be used to generate filters. One-to-many edges will also
        not be used to create filters.

    Args:
        integer: Filters to use for integer properties.
        boolean: Filters to use for boolean properties.
        float: Filters to use for float properties.
        date: Filters to use for date properties.
        datetime: Filters to use for datetime properties.
        string: Filters to use for string properties.
        edge_one_to_one: Filters to use for one-to-one edges, i.e., direct references to other resources.
        by_name: Filters to use for properties with a specific name. This
            overwrites the default filters for the property with the given name.
    """

    integer: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    boolean: tuple[type[dm.Filter], ...] = (dm.filters.Equals,)
    float: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    date: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    datetime: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    string: tuple[type[dm.Filter], ...] = (dm.filters.Equals, dm.filters.In, dm.filters.Prefix)
    edge_one_to_one: tuple[type[dm.Filter], ...] = (dm.filters.Equals, dm.filters.In)
    by_name: dict[str, tuple[type[dm.Filter], ...]] = dataclass_field(
        default_factory=lambda: {"externalId": (dm.filters.Prefix,), "space": (dm.filters.Equals, dm.filters.In)}
    )

    def get(self, type_: dm.PropertyType, prop_name: str | None = None) -> tuple[type[dm.Filter], ...]:
        if prop_name is not None and prop_name in self.by_name:
            return self.by_name[prop_name]

        if isinstance(type_, dm.Int32 | dm.Int64):
            return self.integer
        elif isinstance(type_, dm.Boolean):
            return self.boolean
        elif isinstance(type_, dm.Float32 | dm.Float64):
            return self.float
        elif isinstance(type_, dm.Date):
            return self.date
        elif isinstance(type_, dm.Timestamp):
            return self.datetime
        elif isinstance(type_, dm.Text):
            return self.string
        elif isinstance(type_, dm.DirectRelation):
            return self.edge_one_to_one
        else:
            # Skip unsupported types
            return tuple([])
