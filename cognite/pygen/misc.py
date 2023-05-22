from typing import Optional

from cognite.pygen.dm_clients.misc import to_pascal, to_snake


def to_client_name(name: Optional[str]) -> str:
    """
    Convert a name to a client name.
    >>> to_client_name('Cine')
    'CineClient'
    >>> to_client_name(None)
    'Client'
    """
    return f"{to_pascal(name or '')}Client"


def to_schema_name(name: Optional[str]) -> str:
    """
    Converts a name to a schema name.
    >>> to_schema_name("Cine")
    'cine_schema'
    >>> to_schema_name(None)
    'schema'
    """
    if not name:
        return "schema"
    return f"{to_snake(name)}_schema"
