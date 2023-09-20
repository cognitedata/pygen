from dataclasses import dataclass

from cognite.client import data_modeling as dm


@dataclass
class TypeFilters:
    integer: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    boolean: tuple[type[dm.Filter], ...] = (dm.filters.Equals,)
    float: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    date: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    datetime: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    string: tuple[type[dm.Filter], ...] = (dm.filters.Equals, dm.filters.In, dm.filters.Prefix)
