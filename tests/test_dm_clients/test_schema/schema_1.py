from __future__ import annotations

from typing import Optional

from cognite.dm_clients.domain_modeling import DomainModel
from tests.constants import TestSchemas


class Case(DomainModel):
    __root_model__ = True
    scenario: Optional[Scenario]
    start_time: str
    end_time: str


class Scenario(DomainModel):
    name: str


expected = TestSchemas.case_scenario.read_text()
