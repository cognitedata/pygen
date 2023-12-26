"""
This is a small CLI used for development of Pygen.

"""
import re
from typing import TypeVar

import toml
import typer
from cognite.client._version import __version__ as cognite_sdk_version
from cognite.client.data_classes import (
    FileMetadata,
    FileMetadataList,
    Sequence,
    SequenceList,
    TimeSeries,
    TimeSeriesList,
)
from cognite.client.data_classes.data_modeling import ViewApplyList
from pydantic.version import VERSION as PYDANTIC_VERSION

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
        data_models = example_sdk.load_data_models()
        if len(data_models) == 1:
            data_models = data_models[0]
        sdk_generator = SDKGenerator(
            example_sdk.top_level_package,
            example_sdk.client_name,
            data_models,
            logger=typer.echo,
            default_instance_space=example_sdk.instance_space,
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
            latest = data_model.latest_version()
            # Sorting by to make the output deterministic
            latest.views = sorted(latest.views, key=lambda v: v.external_id)
            for view in latest.views:
                view.properties = dict(sorted(view.properties.items()))
            file_path.write_text(latest.dump_yaml())
            typer.echo(f"Downloaded {file_path.relative_to(REPO_ROOT)}")


@app.command("deploy", help="Deploy all example SDKs to CDF")
def deploy():
    client = load_cognite_client_from_toml("config.toml")
    index = _user_options([", ".join(map(str, example.data_model_ids)) for example in EXAMPLE_SDKS])
    example_sdk = EXAMPLE_SDKS[index]
    spaces = example_sdk.load_spaces()
    client.data_modeling.spaces.apply(spaces)
    for data_model_id in example_sdk.data_model_ids:
        # Containers
        containers = example_sdk.load_containers(data_model_id)
        existing_containers = client.data_modeling.containers.retrieve(containers.as_ids())
        new, changed, unchanged = _difference(existing_containers.as_apply(), containers)
        if changed:
            raise ValueError(f"Containers {changed.as_ids()} require data migration")
        new_containers = client.data_modeling.containers.apply(new)
        for container in new_containers:
            typer.echo(f"Created container {container.external_id} in space {container.space}")
        if unchanged:
            typer.echo(f"{len(unchanged)} containers are unchanged")

        # Views
        views = example_sdk.load_views(data_model_id)
        existing_views = client.data_modeling.views.retrieve(views.as_ids())
        new, changed, unchanged = _difference(existing_views.as_apply(), views)
        is_views_changed = bool(changed or new)
        if changed:
            client.data_modeling.views.delete(changed.as_ids())
        new_views = client.data_modeling.views.apply(new + changed)
        for view in new_views:
            typer.echo(f"Created view {view.external_id} in space {view.space}")
        if unchanged:
            typer.echo(f"{len(unchanged)} views are unchanged")

        # Data Model
        data_model = example_sdk.load_write_model(data_model_id)
        existing_data_model = client.data_modeling.data_models.retrieve(data_model_id).latest_version()
        is_changed = existing_data_model.as_apply() != data_model
        if is_changed or is_views_changed:
            client.data_modeling.data_models.delete(data_model_id)
            new_model = client.data_modeling.data_models.apply(data_model)
            typer.echo(f"Created data model {new_model.external_id} in space {new_model.space}")
        else:
            typer.echo(f"Data model {data_model_id} is unchanged")

        # TimeSeries
        timeseries = example_sdk.load_timeseries(data_model_id)
        existing = client.time_series.retrieve_multiple(
            external_ids=timeseries.as_external_ids(), ignore_unknown_ids=True
        )
        new, changed, unchanged = _difference(existing, timeseries)
        if new:
            created = client.time_series.create(new)
            for ts in created:
                typer.echo(f"Created timeseries {ts.external_id}")
        if changed:
            client.time_series.update(changed)
            for ts in changed:
                typer.echo(f"Updated timeseries {ts.external_id}")
        if unchanged:
            typer.echo(f"{len(unchanged)} timeseries are unchanged")

        # Sequences
        sequences = example_sdk.load_sequences(data_model_id)
        existing = client.sequences.retrieve_multiple(external_ids=sequences.as_external_ids(), ignore_unknown_ids=True)
        new, changed, unchanged = _difference(existing, sequences)
        if new:
            created = client.sequences.create(new)
            for seq in created:
                typer.echo(f"Created sequence {seq.external_id}")
        if changed:
            client.sequences.update(changed)
            for seq in changed:
                typer.echo(f"Updated sequence {seq.external_id}")
        if unchanged:
            typer.echo(f"{len(unchanged)} sequences are unchanged")

        # Files
        files = example_sdk.load_filemetadata(data_model_id)
        existing = client.files.retrieve_multiple(external_ids=files.as_external_ids(), ignore_unknown_ids=True)
        new, changed, unchanged = _difference(existing, files)
        if new:
            created = client.files.create(new)
            for file in created:
                typer.echo(f"Created file {file.external_id}")
        if changed:
            client.files.update(changed)
            for file in changed:
                typer.echo(f"Updated file {file.external_id}")
        if unchanged:
            typer.echo(f"{len(unchanged)} files are unchanged")

        # # Nodes


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


T_DataModeling = TypeVar("T_DataModeling")


def _difference(
    cdf_resources: T_DataModeling, local_resources: T_DataModeling
) -> tuple[T_DataModeling, T_DataModeling, T_DataModeling]:
    """Return new, changed, unchanged"""
    resource_cls_list = type(cdf_resources)
    cdf_resources = {r.as_id() if hasattr(r, "as_id") else r.external_id: r for r in cdf_resources}
    local_resources = {r.as_id() if hasattr(r, "as_id") else r.external_id: r for r in local_resources}

    _clean_cdf_resources(cdf_resources, resource_cls_list)

    new = resource_cls_list([])
    changed = resource_cls_list([])
    unchanged = resource_cls_list([])
    for id_, resource in local_resources.items():
        if id_ not in cdf_resources:
            new.append(resource)
        elif resource != cdf_resources[id_]:
            changed.append(resource)
        else:
            unchanged.append(resource)
    return new, changed, unchanged


def _clean_cdf_resources(cdf_resources: dict, resource_cls_list: type) -> None:
    """Custom cleaning of CDF resources to make them comparable to local resources.

    Args:
        cdf_resources:
        resource_cls_list:

    Returns:

    """
    if resource_cls_list is ViewApplyList:
        # The read version of views includes all properties from the interfaces, but the write version does not.
        # This removes all properties from the write version which are inherited from an interface, so
        # that the comparison is correct.
        interfaces = {parent for view in cdf_resources.values() for parent in view.implements or []}
        property_by_interface = {}
        for view in cdf_resources.values():
            if view.as_id() in interfaces:
                property_by_interface[view.as_id()] = set(view.properties)
        for view in cdf_resources.values():
            for parent in view.implements or []:
                for property_ in property_by_interface[parent]:
                    view.properties.pop(property_, None)

    if resource_cls_list is TimeSeriesList:
        for ts in cdf_resources.values():
            ts: TimeSeries
            ts.created_time = None
            ts.last_updated_time = None
            ts.id = None
            if not ts.metadata:
                ts.metadata = None
            if not ts.security_categories:
                ts.security_categories = None
    if resource_cls_list is SequenceList:
        for seq in cdf_resources.values():
            seq: Sequence
            seq.id = None
            seq.created_time = None
            seq.last_updated_time = None
            if not seq.metadata:
                seq.metadata = None
            for column in seq.columns:
                column.id = None
                column.created_time = None
                column.last_updated_time = None
                if not column.metadata:
                    column.metadata = None
    if resource_cls_list is FileMetadataList:
        for file in cdf_resources.values():
            file: FileMetadata
            file.id = None
            file.created_time = None
            file.last_updated_time = None
            if not file.metadata:
                file.metadata = None


if __name__ == "__main__":
    app()
