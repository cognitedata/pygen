from __future__ import annotations

from dataclasses import dataclass

from jinja2 import Environment, PackageLoader, select_autoescape

from cognite.pygen.dm_clients.misc import to_snake
from cognite.pygen.parser import parse_graphql


@dataclass
class PythonSDK:
    client: str
    schema: str


def to_client_sdk(schema_raw: str, client_name: str, schema_name: str) -> PythonSDK:
    """
    Converts a GraphQL schema to a client-side SDK.

    :param schema_raw: GraphQL schema.
    :param client_name: Name of the client.
    :param schema_name: Name of the schema.
    :return: Client-side SDK
    """
    # Parsing
    models = parse_graphql(schema_raw)

    # Create client.py
    env = Environment(
        loader=PackageLoader("cognite.pygen", "templates"),
        autoescape=select_autoescape(),
    )

    client = env.get_template("client.txt")
    client_py = client.render(
        client_name_snake=to_snake(client_name),
        client_name_camel=client_name,
        schema_name=schema_name,
        # Sort to get consistent results
        models=sorted(models, key=lambda x: x.name),
    )

    # Create schema.py
    schema_tmp = env.get_template("schema.txt")
    ordered = models.topological_order
    ordered[-1].is_root_node = True
    schema_py = schema_tmp.render(
        schema_name=schema_name,
        models=ordered,
    )
    return PythonSDK(_clean_rendered_template(client_py), _clean_rendered_template(schema_py))


def _clean_rendered_template(rendered_template: str) -> str:
    """
    Clean up, adjust new lines and indent.
    """
    return rendered_template.replace("    \n", "\n") + "\n"
