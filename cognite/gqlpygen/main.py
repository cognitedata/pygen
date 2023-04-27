import importlib
import importlib.util
import inspect
import subprocess
import sys
from pathlib import Path
from typing import Optional

import click
import typer
import yaml
from packaging import version

from cognite.dm_clients.config import settings
from cognite.dm_clients.domain_modeling.schema import Schema
from cognite.gqlpygen.generator import to_client_sdk
from cognite.gqlpygen.misc import to_client_name, to_schema_name

app = typer.Typer()


def _hide_pw(secret: str, value: Optional[str] = None) -> str:
    """
    Hide secret from value:

    >>> _hide_pw("SeCrEt", "Here is my SeCrEt password!")
    'Here is my SeC*****... password!'
    >>> _hide_pw("SeCrEtPeRsEcReT")
    'SeC*****...'
    >>> _hide_pw("SeCrEt", "No secret here.")
    'No secret here.'
    >>> _hide_pw("An", "An edge case.")
    'An*****... edge case.'
    >>> _hide_pw("", "Nothing changes.")
    'Nothing changes.'
    """
    return (secret if value is None else value).replace(secret, f"{secret[:3]}*****..." if secret else "")


def _check_cdf_cli() -> None:
    cdf_version_proc = subprocess.run("cdf --version", shell=True, capture_output=True)
    if cdf_version_proc.returncode:
        typer.echo(
            "Error calling 'cdf' command, please see https://docs.cognite.com/cdf/cli/ for installation instructions.",
            err=True,
        )
        sys.exit(1)

    min_version = version.Version("2.0.0")
    installed_version = version.parse(cdf_version_proc.stdout.decode())
    if installed_version < min_version:
        typer.echo(
            f"Too old version of 'cdf' ({installed_version}), at least {min_version} is needed."
            f" Please see https://docs.cognite.com/cdf/cli/ for installation instructions.",
            err=True,
        )
        sys.exit(1)


@app.command("topython", help="Create pydantic schema and Python DM client from a .graphql schema.")
def to_python(
    graphql_schema: Path = typer.Argument(settings.get("local.graphql_schema", ...), help="GraphQL schema to convert"),
    output_dir: Path = typer.Option(
        Path(_graphql_schema).parent if (_graphql_schema := settings.get("local.graphql_schema")) else Path.cwd(),
        help="Directory to write schema.py and client.py to.",
    ),
    name: str = typer.Option(
        settings.get("local.name", ""),
        help="Name of the client and schema, expected to be in pascal case.",
    ),
):
    schema_raw = graphql_schema.read_text()
    client_name = to_client_name(name)
    schema_name = to_schema_name(name)
    sdk = to_client_sdk(schema_raw, client_name, schema_name)
    output_dir = (output_dir or graphql_schema.parent).absolute()
    output_dir.mkdir(exist_ok=True)

    for name, content in sdk.items():
        output = output_dir / name
        output.write_text(content)
        click.echo(f"Wrote file '{output.relative_to(Path.cwd())}'")


@app.command(
    "settings",
    help="Display configuration values from settings.toml, .secrets.toml and/or environment variables."
    " Meant for troubleshooting. Partially hides value of 'client_secret' for security reasons.",
)
def check_settings():
    typer.echo(_hide_pw(settings.get("cognite.client_secret", ""), yaml.safe_dump(settings.as_dict())))


@app.command("togql", help="Input a pydantic schema to create .graphql schema")
def to_gql(
    schema_module: Path = typer.Argument(
        settings.get("local.schema_module", ...),
        help="Pydantic schema to convert. Path to a .py file or Python dot notation ",
    ),
    graphql_schema: Path = typer.Option(settings.get("local.graphql_schema", ...), help="File path for the output."),
    name: Optional[str] = typer.Option(
        settings.get("local.name"),
        help="Name of the client and schema, expected to be in pascal case.",
    ),
):
    if str(schema_module).endswith(".py"):
        click.echo(f"Got file '{schema_module}', trying to import it...")
        module_name = schema_module.stem
        spec = importlib.util.spec_from_file_location(module_name, schema_module)
        module = importlib.util.module_from_spec(spec)  # type:ignore[arg-type]
        sys.modules[module_name] = module
        spec.loader.exec_module(module)  # type:ignore[union-attr]
    else:
        module_name = settings.local.get("schema_module", default=schema_module.stem)
        click.echo(f"Got module '{schema_module}', trying to import it...")
        module = importlib.import_module(module_name)
    click.echo("Import successful")

    if name is None:
        click.echo("Searching for a schema...")
        for schema_name, instance in inspect.getmembers(module):
            if isinstance(instance, Schema):
                click.echo(f"Found schema '{schema_name}'")
                break
        else:
            click.echo("Failed to find schema, exiting..")
            exit(1)
    else:
        schema_name = to_schema_name(name)
        click.echo(f"Got schema name '{schema_name}'")
        try:
            instance = getattr(module, schema_name)
        except AttributeError as exc:
            typer.echo(f"Error: {exc}. Check the --name option and 'schema_module' argument.")
            sys.exit(1)

    graphql_schema.write_text(instance.as_str())
    click.echo(f"Wrote file '{graphql_schema}'")


@app.command("signin", help="Upload a GQL schema to CDF DM data model.")
def signin(
    cdf_cluster: str = typer.Option(settings.get("cognite.cdf_cluster", ...), help="CDF cluster name."),
    tenant_id: str = typer.Option(settings.get("cognite.tenant_id", ...), help="AD tenant ID."),
    client_id: str = typer.Option(settings.get("cognite.client_id", ...), help="AD client ID."),
    project: str = typer.Option(settings.get("cognite.project", ...), help="Name of CDF project."),
):
    client_secret_none = "None (use device flow)"
    client_secret = settings.get("cognite.client_secret")
    if client_secret is None:
        if sys.stdin.isatty():
            client_secret = typer.prompt(
                "Client secret",
                default=client_secret_none,
                type=str,
                hide_input=True,
            )
        else:
            client_secret = sys.stdin.readline().rstrip()
    if client_secret == client_secret_none:
        client_secret = ""
    _check_cdf_cli()
    command = [
        "cdf",
        "signin",
        f"'{project}'",
        f"--cluster='{cdf_cluster}'",
        f"--tenant='{tenant_id}'",
        f"--client-id='{client_id}'",
        (f"--client-secret='{client_secret}'" if client_secret else "--device-code"),
    ]
    typer.echo(f"Executing:\n{_hide_pw(client_secret, ' '.join(command))}")
    subprocess.run(" ".join(command), shell=True)


@app.command("upload", help="Upload a GQL schema to CDF DM data model.")
def upload(
    graphql_schema: Path = typer.Argument(settings.get("local.graphql_schema", ...), help="GraphQL schema to upload."),
    space: str = typer.Option(settings.get("dm_clients.space", ...), help="Space ID in CDF Domain Modeling"),
    data_model: str = typer.Option(
        settings.get("dm_clients.datamodel", ...),
        help="ID of Data Model in CDF Domain Modeling",
    ),
    schema_version: int = typer.Option(
        settings.get("dm_clients.schema_version", ...),
        help="Version of the schema to app or update.",
    ),
):
    _check_cdf_cli()
    command = [
        "cdf",
        "data-models",
        "publish",
        f"--file='{graphql_schema}'",
        f"--space='{space}'",
        f"--external-id='{data_model}'",
        f"--version='{schema_version}'",
    ]
    typer.echo(f"Executing:\n{' '.join(command)}")
    subprocess.run(" ".join(command), shell=True)


def main():
    app()


if __name__ == "__main__":
    main()
