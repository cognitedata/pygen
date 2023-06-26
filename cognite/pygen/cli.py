import getpass
from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.exceptions import CogniteAPIError

from cognite.pygen import SDKGenerator

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
        space: str = typer.Argument(..., help="Location of Data Model"),
        external_id: str = typer.Argument(..., help="External ID of Data Model"),
        version: int = typer.Argument(..., help="Version of Data Model"),
        tenant_id: str = typer.Argument(..., help="Azure Tenant ID for connecting to CDF"),
        client_id: str = typer.Argument(..., help="Azure Client ID for connecting to CDF"),
        client_secret: str = typer.Argument(..., help="Azure Client Secret for connecting to CDF"),
        cdf_cluster: str = typer.Argument(..., help="CDF Cluster to connect to"),
        cdf_project: str = typer.Argument(..., help="CDF Project to connect to"),
        output_dir: str = typer.Option(Path.cwd(), help="Output directory for generated SDK"),
        sdk_name_snake: str = typer.Option("cognite", help="Package name for the generated client."),
        client_name_pascal: str = typer.Option("MyClient", help="Client name for the generated client."),
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
        try:
            data_models = client.data_modeling.data_models.retrieve((space, external_id, version))
        except CogniteAPIError as e:
            typer.echo(f"Error retrieving data model: {e}")
            raise typer.Exit(code=1) from e
        sdk_generator = SDKGenerator(sdk_name_snake, client_name_pascal)
        sdk = sdk_generator.data_model_to_sdk(data_models[0])
        for file_path, file_content in sdk.items():
            path = output_dir / file_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(file_content)

    def main():
        app()

else:

    def main():
        print("THE CLI requires typer to be available, install with `pip install cognite-pygen[cli]")  # noqa


if __name__ == "__main__":
    main()
