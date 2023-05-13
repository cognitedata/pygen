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
class PersonSimple(DomainModel):
    name: str


@cine_schema.register_type(root_type=True)
class MovieSimple(DomainModel):
    title: str
    director: Optional[PersonSimple] = None
    actors: Optional[List[Optional[PersonSimple]]] = []
    producers: Optional[List[Optional[PersonSimple]]] = []
    release: Optional[Timestamp] = None
    meta: Optional[JSONObject] = None
    genres: List[str]


# Keep at the end of the file:
cine_schema.close()


# Render the schema to stdout when executed directly:
if __name__ == "__main__":
    sys.stdout.write(cine_schema.as_str())
