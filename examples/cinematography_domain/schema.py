"""
This is a minimal example schema.
"""

from __future__ import annotations

from typing import List, Optional

from cognite.fdm.general_domain import DomainModel, Schema

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


# Keep at the end of file:
cine_schema.close()


# Render the schema to stdout when executed directly:
if __name__ == "__main__":
    print(cine_schema.as_str())
