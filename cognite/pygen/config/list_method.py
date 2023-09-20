from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field

from cognite.client import data_modeling as dm


@dataclass
class TypeFilters:
    integer: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    boolean: tuple[type[dm.Filter], ...] = (dm.filters.Equals,)
    float: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    date: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    datetime: tuple[type[dm.Filter], ...] = (dm.filters.Range,)
    string: tuple[type[dm.Filter], ...] = (dm.filters.Equals, dm.filters.In, dm.filters.Prefix)
    by_name: dict[str, tuple[type[dm.Filter], ...]] = dataclass_field(
        default_factory=lambda: {"externalId": (dm.filters.Prefix,)}
    )

    def get(self, type_: dm.PropertyType, prop_name: str | None = None) -> tuple[type[dm.Filter], ...]:
        if prop_name is not None and prop_name in self.by_name:
            return self.by_name[prop_name]

        if isinstance(type_, (dm.Int32, dm.Int64)):
            return self.integer
        elif isinstance(type_, dm.Boolean):
            return self.boolean
        elif isinstance(type_, (dm.Float32, dm.Float64)):
            return self.float
        elif isinstance(type_, dm.Date):
            return self.date
        elif isinstance(type_, dm.Timestamp):
            return self.datetime
        elif isinstance(type_, dm.Text):
            return self.string
        else:
            raise ValueError(f"Filter type {type_} is not currently not supported.")
