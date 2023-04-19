from cognite.dm_clients.misc import to_pascal, to_snake


def to_client_name(name: str) -> str:
    """
    Convert a name to a client name.
    >>> to_client_name('Cine')
    'CineClient'
    """
    return f"{to_pascal(name)}Client"


def to_schema_name(name: str) -> str:
    """
    Converts a name to a schema name.
    >>> to_schema_name("Cine")
    'cine_schema'
    """
    return f"{to_snake(name)}_schema"
