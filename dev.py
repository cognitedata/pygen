"""
This is a small CLI used for development of Pygen.

"""
import re

import toml
import typer
from cognite.client._version import __version__ as cognite_sdk_version
from cognite.client.data_classes.data_modeling import DataModel, SpaceApply
from pydantic.version import VERSION as PYDANTIC_VERSION
from yaml import safe_load

from cognite.pygen._generator import SDKGenerator, write_sdk_to_disk
from cognite.pygen.utils.cdf import _user_options, load_cognite_client_from_toml
from tests.constants import EXAMPLE_SDKS, EXAMPLES_DIR, REPO_ROOT

app = typer.Typer(
    add_completion=False,
    help=__doc__,
    pretty_exceptions_short=False,
    pretty_exceptions_show_locals=False,
    pretty_exceptions_enable=False,
)


@app.command("generate", help=f"Generate all example SDKs in directory '{EXAMPLES_DIR.relative_to(REPO_ROOT)}/'")
def generate_sdks(
    overwrite: bool = typer.Option(
        False, help="Whether to overwrite the files expected to be manually maintained in the examples"
    ),
    sdk_name: str = typer.Option(None, "--sdk", help="Generate only the specified SDK"),
):
    for example_sdk in EXAMPLE_SDKS:
        if not example_sdk.generate_sdk:
            continue
        if sdk_name is not None and not example_sdk.client_name.casefold().startswith(sdk_name.casefold()):
            continue
        typer.echo(f"Generating {example_sdk.client_name} SDK...")
        data_models = [DataModel.load(safe_load(dms_file.read_text())[0]) for dms_file in example_sdk.dms_files]
        if len(data_models) == 1:
            data_models = data_models[0]
        sdk_generator = SDKGenerator(
            example_sdk.top_level_package, example_sdk.client_name, data_models, logger=typer.echo
        )

        sdk = sdk_generator.generate_sdk()
        manual_files = []
        if overwrite is not True:
            for manual_file in example_sdk.manual_files:
                manual_path = manual_file.relative_to(EXAMPLES_DIR)
                popped = sdk.pop(manual_path, None)
                if popped is None:
                    typer.echo(f"Could not find {manual_path} in generated SDK", err=True, color=True)
                else:
                    manual_files.append(manual_path)
        write_sdk_to_disk(sdk, EXAMPLES_DIR, overwrite=True, format_code=True)
        typer.echo(f"{example_sdk.client_name} SDK Created in {example_sdk.client_dir}")
        if manual_files:
            typer.echo(
                f"The following files were not updated, as they are expected to be changed manually: {manual_files}"
            )
        else:
            typer.echo("All files updated! Including files assumed to be manually maintained.")
        typer.echo("\n")
    typer.echo("All SDKs Created!")


@app.command("download", help="Download the DMS representation of all example SDKs")
def download():
    client = load_cognite_client_from_toml("config.toml")
    for example_sdk in EXAMPLE_SDKS:
        for data_model_id in example_sdk.data_model_ids:
            data_model = client.data_modeling.data_models.retrieve(data_model_id, inline_views=True)
            if not data_model:
                raise ValueError(f"Failed to retrieve {data_model_id}")
            file_path = example_sdk.read_model_path(data_model_id)
            file_path.write_text(data_model.dump_yaml())
            typer.echo(f"Downloaded {file_path.relative_to(REPO_ROOT)}")


@app.command("deploy", help="Deploy all example SDKs to CDF")
def deploy():
    client = load_cognite_client_from_toml("config.toml")
    index = _user_options([", ".join(map(str, example.data_models)) for example in EXAMPLE_SDKS])
    example_sdk = EXAMPLE_SDKS[index]
    data_models = example_sdk.load_data_models()
    spaces = list({model.space for model in data_models})
    client.data_modeling.spaces.apply([SpaceApply(space) for space in spaces])
    for data_model_id in example_sdk.data_model_ids:
        containers = example_sdk.load_containers(data_model_id)
        new_containers = client.data_modeling.containers.apply(containers)
        for container in new_containers:
            typer.echo(f"Created container {container.external_id} in space {container.space}")
        views = example_sdk.load_views(data_model_id)
        new_views = client.data_modeling.views.apply(views)
        for view in new_views:
            typer.echo(f"Created view {view.external_id} in space {view.space}")
        data_model = example_sdk.load_write_model(data_model_id)
        new_model = client.data_modeling.data_models.apply(data_model)
        typer.echo(f"Created data model {new_model.external_id} in space {new_model.space}")


@app.command("list", help="List all example files which are expected to be changed manually")
def list_manual_files():
    for example_sdk in EXAMPLE_SDKS:
        if not example_sdk.generate_sdk:
            continue
        typer.echo(f"{example_sdk.client_name} SDK:")
        for manual_file in example_sdk.manual_files:
            typer.echo(f" - {manual_file.relative_to(EXAMPLES_DIR)}")


@app.command(
    "bump", help="Bump the version of Pygen. This also updates the cognite-sdk and pydantic version in all examples"
)
def bump(major: bool = False, minor: bool = False, patch: bool = False, skip: bool = False):
    if sum([major, minor, patch, skip]) != 1:
        raise typer.BadParameter("Exactly one of --major, --minor, --patch, or --skip must be set")

    pyproject_toml = REPO_ROOT / "pyproject.toml"
    version_py = REPO_ROOT / "cognite" / "pygen" / "_version.py"
    api_client_files_v2 = list((REPO_ROOT / "examples").glob("**/_api_client.py"))
    api_client_files_v1 = list((REPO_ROOT / "examples-pydantic-v1").glob("**/_api_client.py"))
    api_client_files = api_client_files_v1 + api_client_files_v2
    current_version = toml.loads(pyproject_toml.read_text())["tool"]["poetry"]["version"]

    current_major, current_minor, current_patch = (int(x) for x in current_version.split("."))
    if major:
        current_major += 1
        current_minor = 0
        current_patch = 0
    elif minor:
        current_minor += 1
        current_patch = 0
    elif patch:
        current_patch += 1
    new_version = f"{current_major}.{current_minor}.{current_patch}"
    typer.echo(f"Bumping version from {current_version} to {new_version}...")
    typer.echo(f"...and setting cognite-sdk={cognite_sdk_version} and pydantic={PYDANTIC_VERSION} in examples.")
    answer = typer.confirm("Are you sure you want to continue?")
    if not answer:
        typer.echo("Aborting")
        raise typer.Abort()

    if current_version == cognite_sdk_version or current_version == PYDANTIC_VERSION:
        raise ValueError(f"Edge case not handled: {current_version=}, {cognite_sdk_version=}, {PYDANTIC_VERSION=}")

    for file in [pyproject_toml, version_py, *api_client_files]:
        content = file.read_text()
        if not skip:
            content = content.replace(current_version, new_version)
        content = re.sub(r"cognite-sdk = \d+.\d+.\d+", f"cognite-sdk = {cognite_sdk_version}", content)
        if file not in api_client_files_v1:
            # pydantic v1 is frozen at 1.10.7
            content = re.sub(r"pydantic = \d+.\d+.\d+", f"pydantic = {PYDANTIC_VERSION}", content)
        file.write_text(content)
        typer.echo(f"Updated {file.relative_to(REPO_ROOT)}, replaced {current_version} with {new_version}.")
    typer.echo("Done")


@app.command(
    "overwrite-index", help="README.md and docs/index.md must match. This commands copies from README.md to index.md"
)
def overwrite_index():
    readme = (REPO_ROOT / "README.md").read_text()
    index_path = REPO_ROOT / "docs" / "index.md"
    index = index_path.read_text()

    copy = _remove_top_lines(readme, 2)
    new_index = "\n".join(index.split("\n")[:1] + copy.split("\n"))
    index_path.write_text(new_index)


def _remove_top_lines(text: str, lines: int) -> str:
    return "\n".join(text.split("\n")[lines:])


if __name__ == "__main__":
    app()
