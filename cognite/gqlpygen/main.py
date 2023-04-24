import importlib
import importlib.util
import inspect
import sys
from pathlib import Path

import click
import typer

from cognite.dm_clients.domain_modeling.schema import Schema
from cognite.gqlpygen.generator import to_client_sdk
from cognite.gqlpygen.misc import to_client_name, to_schema_name

app = typer.Typer()


@app.command("topython", help="Input a .graphql schema to create pydantic schema.")
def to_python(
    graphql_schema: Path = typer.Argument(..., help="GraphQL schema to convert"),
    name: str = typer.Option(
        "MySchema", "--name", help="Name of the client and schema, expected to be in pascal case."
    ),
):
    schema_raw = graphql_schema.read_text()
    client_name = to_client_name(name)
    schema_name = to_schema_name(name)
    sdk = to_client_sdk(schema_raw, client_name, schema_name)

    for name, content in sdk.items():
        output = Path.cwd() / name
        output.write_text(content)
        click.echo(f"Wrote file {output.relative_to(Path.cwd())}")


@app.command("togql", help="Input a pydantic schema to create .graphql schema")
def to_gql(pydantic_schema: Path = typer.Argument(..., help="Pydantic schema to convert")):
    module_name = pydantic_schema.stem
    click.echo(f"Got file {pydantic_schema}, trying to import it...")
    spec = importlib.util.spec_from_file_location(module_name, pydantic_schema)
    module = importlib.util.module_from_spec(spec)  # type: ignore [arg-type]
    sys.modules[module_name] = module
    spec.loader.exec_module(module)  # type: ignore [union-attr]
    click.echo("Import successful")

    click.echo("Searching for a schema...")
    for schema_name, instance in inspect.getmembers(module):
        if isinstance(instance, Schema):
            click.echo(f"Found schema {schema_name!r}")
            break
    else:
        click.echo("Failed to find schema, exiting..")
        exit(1)

    output = Path.cwd() / "schema.graphql"
    output.write_text(instance.as_str())
    click.echo(f"Wrote file {output}")


def main():
    app()
