from __future__ import annotations

from typing import Optional

from cognite.dm_clients.domain_modeling import DomainModel


class Case(DomainModel):
    __root_model__ = True
    scenario: Optional[Scenario]
    start_time: str
    end_time: str


class Scenario(DomainModel):
    name: str


expected = """
# THIS FILE IS AUTO-GENERATED!
# Use `dm_clients schema render` to update it, see `dm_clients --help` for more information.


type Case {
  scenario: Scenario
  start_time: String!
  end_time: String!
}

type Scenario {
  name: String!
}
"""
