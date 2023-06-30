import getpass
from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.exceptions import CogniteAPIError
from typing_extensions import Annotated

from cognite.pygen import SDKGenerator, write_sdk_to_disk

try:
    import typer
except ImportError:
    _has_typer = False
    typer = None
else:
    _has_typer = True


if _has_typer:
    app = typer.Typer()

    @app.command(help="Generate a Python SDK from Data Model")
    def generate(
        space: Annotated[str, typer.Option(..., help="Location of Data Model")],
        external_id: Annotated[str, typer.Option(..., help="External ID of Data Model")],
        version: Annotated[str, typer.Option(..., help="Version of Data Model")],
        tenant_id: Annotated[str, typer.Option(..., help="Azure Tenant ID for connecting to CDF")],
        client_id: Annotated[str, typer.Option(..., help="Azure Client ID for connecting to CDF")],
        client_secret: Annotated[str, typer.Option(..., help="Azure Client Secret for connecting to CDF")],
        cdf_cluster: Annotated[str, typer.Option(..., help="CDF Cluster to connect to")],
        cdf_project: Annotated[str, typer.Option(..., help="CDF Project to connect to")],
        output_dir: Path = typer.Option(Path.cwd(), help="Output directory for generated SDK"),
        top_level_package: str = typer.Option("my_domain.client", help="Package name for the generated client."),
        client_name: str = typer.Option("MyClient", help="Client name for the generated client."),
    ):
        base_url = f"https://{cdf_cluster}.cognitedata.com/"
        credentials = OAuthClientCredentials(
            token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=[f"{base_url}.default"],
        )
        config = ClientConfig(
            project=cdf_project,
            credentials=credentials,
            client_name=getpass.getuser(),
            base_url=base_url,
        )
        client = CogniteClient(config)

        model_id = (space, external_id, version)
        try:
            data_models = client.data_modeling.data_models.retrieve(model_id, inline_views=True)
            data_model = data_models[0]
        except CogniteAPIError as e:
            typer.echo(f"Error retrieving data model: {e}")
            raise typer.Exit(code=1) from e
        except IndexError as e:
            typer.echo(f"Cannot find {model_id}")
            raise typer.Exit(code=1) from e
        typer.echo(f"Successfully retrieved data model {space}/{external_id}/{version}")
        sdk_generator = SDKGenerator(top_level_package, client_name)
        sdk = sdk_generator.data_model_to_sdk(data_model)
        typer.echo(f"Writing SDK to {output_dir}")
        write_sdk_to_disk(sdk, output_dir)
        typer.echo("Done!")

    def main():
        app()

else:

    def main():
        print("THE CLI requires typer to be available, install with `pip install cognite-pygen[cli]")  # noqa


if __name__ == "__main__":
    main()
