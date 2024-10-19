"""This is a small CLI used for Pygen development."""

import re
from collections import defaultdict
from datetime import datetime
from typing import TypeVar

import toml
import typer
from cognite.client import data_modeling as dm
from cognite.client._version import __version__ as cognite_sdk_version
from pydantic.version import VERSION as PYDANTIC_VERSION

from cognite.pygen._generator import SDKGenerator, generate_typed, write_sdk_to_disk
from cognite.pygen.utils import MockGenerator
from cognite.pygen.utils.cdf import load_cognite_client_from_toml
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
            generate_typed(data_models, output_file, include_views=include_views, implements="composition")
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
            file_path.parent.mkdir(exist_ok=True, parents=True)
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


@app.command("mock", help="Generate mock data for all example SDKs")
def mock():
    for example_sdk in EXAMPLE_SDKS:
        if not example_sdk.download_nodes:
            typer.echo(f"Skipping {example_sdk.client_name} as it does not download nodes")
            continue
        typer.echo(f"Generating mock data for {example_sdk.client_name}...")
        model = example_sdk.load_data_model()
        MockGenerator(
            model.views,
            example_sdk.instance_space,
            default_config="faker",
        )


@app.command(
    "bump", help="Bump the version of Pygen. This also updates the cognite-sdk and pydantic version in all examples"
)
def bump(major: bool = False, minor: bool = False, patch: bool = False, skip: bool = False):
    if sum([major, minor, patch, skip]) != 1:
        raise typer.BadParameter("Exactly one of --major, --minor, --patch, or --skip must be set")

    pyproject_toml = REPO_ROOT / "pyproject.toml"
    version_py = REPO_ROOT / "cognite" / "pygen" / "_version.py"
    api_client_files = list((REPO_ROOT / "examples").glob("**/_api_client.py"))
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
