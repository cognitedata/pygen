from pathlib import Path
from typing import Callable

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from typing_extensions import Annotated

from cognite.pygen import SDKGenerator, write_sdk_to_disk
from cognite.pygen._settings import PygenSettings, get_cognite_client, load_settings

try:
    import typer
except ImportError:
    _has_typer = False
    typer = None
else:
    _has_typer = True


def create_sdk(
    client: CogniteClient,
    space: str,
    external_id: str,
    version: str,
    output_dir: Path,
    top_level_package: str,
    client_name: str,
    logger: Callable[[str], None] = None,
):
    model_id = (space, external_id, version)
    try:
        data_models = client.data_modeling.data_models.retrieve(model_id, inline_views=True)
        data_model = data_models[0]
    except CogniteAPIError as e:
        logger(f"Error retrieving data model: {e}")
        raise e
    except IndexError as e:
        logger(f"Cannot find {model_id}")
        raise e
    logger(f"Successfully retrieved data model {space}/{external_id}/{version}")
    sdk_generator = SDKGenerator(top_level_package, client_name, data_model, logger=typer.echo)
    sdk = sdk_generator.generate_sdk()
    logger(f"Writing SDK to {output_dir}")
    write_sdk_to_disk(sdk, output_dir)
    logger("Done!")


if _has_typer:
    app = typer.Typer()

    pyproject_toml = Path.cwd() / "pyproject.toml"
    if pyproject_toml.exists():
        typer.echo("Found pyproject.toml loading configuration.")
        settings = load_settings(pyproject_toml)

        @app.command(help="Generate a Python SDK from Data Model")
        def generate(
            client_secret: Annotated[str, typer.Option(..., help="Azure Client Secret for connecting to CDF")],
            space: str = typer.Option(default=settings.space.default, help=settings.space.help),
            external_id: str = typer.Option(default=settings.external_id.default, help=settings.external_id.help),
            version: str = typer.Option(default=settings.version.default, help=settings.version.help),
            tenant_id: str = typer.Option(default=settings.tenant_id.default, help=settings.tenant_id.help),
            client_id: str = typer.Option(default=settings.client_id.default, help=settings.client_id.help),
            cdf_cluster: str = typer.Option(default=settings.cdf_cluster.default, help=settings.cdf_cluster.help),
            cdf_project: str = typer.Option(default=settings.cdf_project.default, help=settings.cdf_project.help),
            output_dir: Path = typer.Option(Path.cwd(), help=settings.output_dir.help),
            top_level_package: str = typer.Option(
                settings.top_level_package.default, help=settings.top_level_package.help
            ),
            client_name: str = typer.Option(settings.client_name.default, help=settings.client_name.help),
        ):
            client = get_cognite_client(cdf_project, cdf_cluster, tenant_id, client_id, client_secret)
            try:
                create_sdk(
                    client, space, external_id, version, output_dir, top_level_package, client_name, logger=typer.echo
                )
            except (CogniteAPIError, IndexError) as e:
                raise typer.Exit(code=1) from e

    else:
        settings = PygenSettings()

        @app.command(help="Generate a Python SDK from Data Model")
        def generate(
            space: Annotated[str, typer.Option(..., help=settings.space.help)],
            external_id: Annotated[str, typer.Option(..., help=settings.external_id.help)],
            version: Annotated[str, typer.Option(..., help=settings.version.help)],
            tenant_id: Annotated[str, typer.Option(..., help=settings.tenant_id.help)],
            client_id: Annotated[str, typer.Option(..., help=settings.client_id.help)],
            client_secret: Annotated[str, typer.Option(..., help="Azure Client Secret for connecting to CDF")],
            cdf_cluster: Annotated[str, typer.Option(..., help=settings.cdf_cluster.help)],
            cdf_project: Annotated[str, typer.Option(..., help=settings.cdf_project.help)],
            output_dir: Path = typer.Option(Path.cwd(), help=settings.output_dir.help),
            top_level_package: str = typer.Option(
                settings.top_level_package.default, help=settings.top_level_package.help
            ),
            client_name: str = typer.Option(settings.client_name.default, help=settings.client_name.help),
        ):
            client = get_cognite_client(cdf_project, cdf_cluster, tenant_id, client_id, client_secret)
            try:
                create_sdk(
                    client, space, external_id, version, output_dir, top_level_package, client_name, logger=typer.echo
                )
            except (CogniteAPIError, IndexError) as e:
                raise typer.Exit(code=1) from e

    def main():
        app()

else:

    def main():
        print("THE CLI requires typer to be available, install with `pip install cognite-pygen[cli]")  # noqa


if __name__ == "__main__":
    main()
