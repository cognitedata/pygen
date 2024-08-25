"""This is a small CLI used for Pygen development."""

import re
from collections import defaultdict
from datetime import datetime
from typing import TypeVar

import toml
import typer
from cognite.client import data_modeling as dm
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

from cognite.pygen._generator import SDKGenerator, generate_typed, write_sdk_to_disk
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

        if example_sdk.is_typed:
            output_file = example_sdk.client_dir / "typed.py"
            include_views = {dm.ViewId(data_models.space, t, "1") for t in example_sdk.typed_classes} or None
            generate_typed(data_models, output_file, include_views=include_views)
            continue

        sdk_generator = SDKGenerator(
            example_sdk.top_level_package,
            example_sdk.client_name,
            data_models,
            logger=typer.echo,
            default_instance_space=example_sdk.instance_space,
        )

        sdk = sdk_generator.generate_sdk()
        write_sdk_to_disk(
            sdk,
            EXAMPLES_DIR,
            overwrite=True,
            logger=print,
            format_code=True,
            top_level_package=example_sdk.top_level_package,
        )
        typer.echo(f"{example_sdk.client_name} SDK Created in {example_sdk.client_dir}")
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
                typer.echo(f"Data Model {data_model_id} has not been deployed to CDF, skipping")
                continue
            file_path = example_sdk.read_model_path(data_model_id)
            latest = data_model.latest_version()
            # Sorting by to make the output deterministic
            latest.views = sorted(latest.views, key=lambda v: v.external_id)
            for view in latest.views:
                view.properties = dict(sorted(view.properties.items()))
            file_path.write_text(latest.dump_yaml())
            typer.echo(f"Downloaded {file_path.relative_to(REPO_ROOT)}")

            if not example_sdk.download_nodes:
                continue

            is_space: dm.filters.Filter | None = None
            if example_sdk.instance_space:
                is_space = dm.filters.Equals(["node", "space"], example_sdk.instance_space)
            nodes = dm.NodeList([])
            for view in latest.views:
                nodes.extend(client.data_modeling.instances.list("node", filter=is_space, limit=100, sources=[view]))
            nodes = dm.NodeList(sorted(nodes, key=lambda n: n.external_id))
            nodes = _remove_duplicate_nodes(nodes)
            _isoformat_timestamps(nodes)
            file_path = example_sdk.read_node_path(data_model_id)
            file_path.write_text(nodes.dump_yaml())
            typer.echo(f"Downloaded {len(nodes)} nodes to {file_path.relative_to(REPO_ROOT)}")


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
        new, changed, unchanged = _difference(existing_containers.as_write(), containers)
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
        new, changed, unchanged = _difference(existing_views.as_write(), views)
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
        existing_data_models = client.data_modeling.data_models.retrieve(data_model_id)
        if not existing_data_models:
            is_changed = True
        else:
            is_changed = existing_data_models.latest_version().as_write() != data_model
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
            for n in new:
                created, _ = client.files.create(n)
                typer.echo(f"Created file {created.external_id}")
        if changed:
            client.files.update(changed)
            for file in changed:
                typer.echo(f"Updated file {file.external_id}")
        if unchanged:
            typer.echo(f"{len(unchanged)} files are unchanged")

        # Nodes
        nodes = example_sdk.load_nodes(data_model_id, isoformat_dates=True)
        for node in nodes:
            # Bug in SDK. Should not be necessary to set to None
            node.sources = node.sources or None
        result = client.data_modeling.instances.apply(nodes=nodes)
        changed = [node for node in result.nodes if node.was_modified]
        unchanged = [node for node in result.nodes if not node.was_modified]
        for node in changed:
            typer.echo(f"Created node {node.as_id()}")
        if unchanged:
            typer.echo(f"{len(unchanged)} nodes are unchanged")

        # Edges
        edges = example_sdk.load_edges(data_model_id)
        for edge in edges:
            # Bug in SDK. Should not be necessary to set to None
            edge.sources = edge.sources or None
        result = client.data_modeling.instances.apply(
            edges=edges, auto_create_start_nodes=True, auto_create_end_nodes=True
        )
        changed = [edge for edge in result.edges if edge.was_modified]
        unchanged = [edge for edge in result.edges if not edge.was_modified]
        for edge in changed:
            typer.echo(f"Created edge {edge.as_id()}")
        if unchanged:
            typer.echo(f"{len(unchanged)} edges are unchanged")

    # After deployment, we should download the read version of the data model
    download()


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
    quickstart_streamlit = REPO_ROOT / "docs" / "quickstart" / "cdf_streamlit.md"

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
    for file in [quickstart_streamlit]:
        content = file.read_text()
        if not skip:
            content = content.replace(current_version, new_version)
        content = re.sub(r"cognite-sdk==\d+.\d+.\d+", f"cognite-sdk=={cognite_sdk_version}", content)
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
            file.uploaded = None
            if not file.metadata:
                file.metadata = None


def _remove_duplicate_nodes(nodes: dm.NodeList) -> dm.NodeList:
    """Remove duplicate nodes from the list.

    Args:
        nodes:

    Returns:

    """
    nodes_by_id = defaultdict(list)
    for node in nodes:
        nodes_by_id[node.as_id()].append(node)

    output = dm.NodeList([])

    for nodes in nodes_by_id.values():
        if len(nodes) == 1:
            output.append(nodes[0])
            continue
        # The node with the most properties is the child node that has implemented all interfaces
        # (and thus have all values of their parent interfaces).
        keep: dm.Node = max(nodes, key=lambda n: sum(len(props) for props in n.properties.values()))
        output.append(keep)
    return output


def _isoformat_timestamps(nodes: dm.NodeList):
    """Convert all timestamps to isoformat.

    Args:
        nodes:

    Returns:

    """
    for node in nodes:
        node: dm.Node
        for properties in node.properties.values():
            for key in list(properties):
                value = properties[key]
                if isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, datetime):
                            properties[key][i] = item.isoformat(timespec="milliseconds")
                        elif isinstance(item, str):
                            try:
                                parsed = datetime.strptime(item, "%Y-%m-%dT%H:%M:%S%z")
                            except ValueError:
                                continue
                            properties[key][i] = parsed.isoformat(timespec="milliseconds")
                elif isinstance(value, datetime):
                    properties[key] = value.isoformat(timespec="milliseconds")
                elif isinstance(value, str):
                    try:
                        # 2023-04-16T18:28:09.000+00:00
                        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
                    except ValueError:
                        continue
                    properties[key] = parsed.isoformat(timespec="milliseconds")


if __name__ == "__main__":
    app()
