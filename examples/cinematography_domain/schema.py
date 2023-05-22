"""
This is a minimal example schema.
"""

from __future__ import annotations

import logging
import sys
from typing import List, Optional

from cognite.pygen.dm_clients.custom_types import JSONObject, Timestamp
from cognite.pygen.dm_clients.domain_modeling import DomainModel, Schema

logger = logging.getLogger(__name__)

cine_schema: Schema[DomainModel] = Schema()


@cine_schema.register_type
class Person(DomainModel):
    name: str


@cine_schema.register_type(root_type=True)
class Movie(DomainModel):
    title: str
    director: Optional[Person] = None
    actors: Optional[List[Optional[Person]]] = []
    producers: Optional[List[Optional[Person]]] = []
    release: Optional[Timestamp] = None
    meta: Optional[JSONObject] = None
    genres: List[str]


# Keep at the end of the file:
cine_schema.close()


# Render the schema to stdout when executed directly:
if __name__ == "__main__":
    sys.stdout.write(cine_schema.as_str())
