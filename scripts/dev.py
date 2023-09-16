"""
This is a small CLI used for development of Pygen.

"""
import typer

from cognite.pygen import SDKGenerator, write_sdk_to_disk
from cognite.pygen.utils.cdf import load_cognite_client_from_toml
from tests.constants import EXAMPLE_SDKS, EXAMPLES_DIR, REPO_ROOT
from cognite.client.data_classes.data_modeling import DataModel
from yaml import safe_load, safe_dump

app = typer.Typer(add_completion=False, help=__doc__)


@app.command("generate-sdks", help=f"Generate all example SDKs in directory '{EXAMPLES_DIR.relative_to(REPO_ROOT)}/'")
def generate_sdks(overwrite_manual_files: bool = typer.Option(False, help="Overwrite manual files in examples")):
    for example_sdk in EXAMPLE_SDKS:
        data_models = [DataModel.load(safe_load(dms_file.read_text())) for dms_file in example_sdk.dms_files]
        sdk_generator = SDKGenerator(
            example_sdk.top_level_package, example_sdk.client_name, data_models, logger=typer.echo
        )

        sdk = sdk_generator.generate_sdk()
        manual_files = []
        if overwrite_manual_files is True:
            for manual_file in example_sdk.manual_files:
                manual_path = manual_file.relative_to(EXAMPLES_DIR)
                popped = sdk.pop(manual_path, None)
                if popped is None:
                    typer.echo(f"Could not find {manual_path} in generated SDK", err=True, color=True)
                else:
                    manual_files.append(popped)
        write_sdk_to_disk(sdk, EXAMPLES_DIR, overwrite=True, format_code=True)
        typer.echo(f"{example_sdk.client_name} SDK Created in {example_sdk.client_dir}")
        if manual_files:
            typer.echo(
                f"The following files were not updated, as they are expected to be changed manually: {manual_files}"
            )
    typer.echo("All SDKs Created!")


@app.command("download", help="Download the DMS representation of all example SDKs")
def download():
    client = load_cognite_client_from_toml("config.toml")
    for example_sdk in EXAMPLE_SDKS:
        for datamodel_id, dms_file in zip(example_sdk.data_models, example_sdk.dms_files):
            dms_model = client.data_modeling.data_models.retrieve(model_id=datamodel_id, inline_views=True)
            dms_file.write_text(safe_dump(dms_model.dump(), sort_keys=False))
            typer.echo(f"Downloaded {dms_file.relative_to(REPO_ROOT)}")


@app.command(
    "bump", help="Bump the version of Pygen. This also updates the Python-SDK and Pydantic version in all examples"
)
def bump(major: bool = False, minor: bool = False, patch: bool = False):
    if sum([major, minor, patch]) != 1:
        raise typer.BadParameter("Exactly one of --major, --minor, or --patch must be set")
    last_version = "0.17.6"
    new_version = "0.17.7"

    pyproject_toml = REPO_ROOT / "pyproject.toml"
    version_py = REPO_ROOT / "cognite" / "pygen" / "_version.py"
    api_client_files = list(EXAMPLES_DIR.glob("**/_api_client.py")) + list(
        EXAMPLES_DIR_PYDANTIC_V1.glob("**/_api_client.py")
    )

    for file in [pyproject_toml, version_py, *api_client_files]:
        content = file.read_text().replace(last_version, new_version)
        file.write_text(content)
        print(f"Updated {file.relative_to(REPO_ROOT)}, replacing {last_version} with {new_version}.")
    print("Done")


@app.command("readme", help="Update the README.md file")
def readme(readme: bool, index: bool):
    if sum([readme, index]) != 1:
        raise typer.BadParameter("Exactly one of --readme or --index must be set")


if __name__ == "__main__":
    app()
