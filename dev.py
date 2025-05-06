"""This is a small CLI used for Pygen development."""

import time
from collections import defaultdict
from datetime import datetime
from multiprocessing import Pool
from typing import Literal, TypeVar, get_args

import marko
import marko.block
import marko.element
import marko.inline
import typer
from cognite.client import data_modeling as dm
from packaging.version import Version, parse

from cognite.pygen._generator import SDKGenerator, generate_typed, write_sdk_to_disk
from cognite.pygen.utils import MockGenerator
from cognite.pygen.utils.cdf import load_cognite_client_from_toml
from tests.constants import DATA_WRITE_DIR, EXAMPLE_SDKS, EXAMPLES_DIR, REPO_ROOT, ExampleSDK

app = typer.Typer(
    add_completion=False,
    help=__doc__,
    pretty_exceptions_short=False,
    pretty_exceptions_show_locals=False,
    pretty_exceptions_enable=False,
)

VALID_CHANGELOG_HEADERS = {"Added", "Changed", "Removed", "Fixed", "Improved"}
BUMP_OPTIONS = Literal["major", "minor", "patch", "skip"]
VALID_BUMP_OPTIONS = get_args(BUMP_OPTIONS)
LAST_GIT_MESSAGE_FILE = REPO_ROOT / "last_git_message.txt"
CHANGELOG_ENTRY_FILE = REPO_ROOT / "last_changelog_entry.md"
LAST_VERSION = REPO_ROOT / "last_version.txt"
VERSION_PLACEHOLDER = "0.0.0"
VERSION_FILES = (
    REPO_ROOT / "pyproject.toml",
    REPO_ROOT / "cognite" / "pygen" / "_version.py",
)


@app.command("generate", help=f"Generate all example SDKs in directory '{EXAMPLES_DIR.relative_to(REPO_ROOT)}/'")
def generate_sdks(
    sdk_name: str = typer.Option(None, "--sdk", help="Generate only the specified SDK"),
):
    t0 = time.time()
    sdks_to_generate = (example_sdk for example_sdk in EXAMPLE_SDKS if example_sdk.generate_sdk)
    if sdk_name is not None:
        sdks_to_generate = (
            example_sdk
            for example_sdk in sdks_to_generate
            if not example_sdk.client_name.casefold().startswith(sdk_name.casefold())
        )
    sdks = list(sdks_to_generate)
    with Pool(min(8, len(sdks))) as pool:
        pool.map(_generate_sdk, sdks)

    elapsed = time.time() - t0
    typer.echo(f"All SDKs Created in {elapsed:.1f} seconds!")


def _generate_sdk(example_sdk: ExampleSDK) -> None:
    typer.echo(f"Generating {example_sdk.client_name} SDK...")
    data_models = example_sdk.load_data_models()
    if len(data_models) == 1:
        data_models = data_models[0]

    if example_sdk.is_typed:
        output_file = example_sdk.client_dir / "typed.py"
        include_views = {dm.ViewId(data_models.space, t, "1") for t in example_sdk.typed_classes} or None
        generate_typed(data_models, output_file, include_views=include_views, implements="composition")
        return

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
        example_sdk.client_dir,
        overwrite=True,
        logger=print,
        format_code=True,
    )
    typer.echo(f"{example_sdk.client_name} SDK Created in {example_sdk.client_dir}")
    typer.echo("All files updated! Including files assumed to be manually maintained.")
    typer.echo("\n")


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

            parent_views = {parent for view in latest.views for parent in view.implements or []}
            nodes_by_id: dict[dm.NodeId, list] = defaultdict(list)
            for view in latest.views:
                if view.used_for == "edge" or view.as_id() in parent_views:
                    continue
                view_nodes = client.data_modeling.instances.list("node", filter=is_space, limit=100, sources=[view])
                for node in view_nodes:
                    nodes_by_id[node.as_id()].append(node)
            node_list = dm.NodeList([])
            for nodes in nodes_by_id.values():
                if len(nodes) == 1:
                    node_list.append(nodes[0])
                    continue
                # The node with the most properties is the child node that has implemented all interfaces
                # (and thus have all values of their parent interfaces).
                keep: dm.Node = max(nodes, key=lambda n: sum(len(props) for props in n.properties.values()))
                node_list.append(keep)
            node_list = dm.NodeList(sorted(node_list, key=lambda n: n.external_id))
            _isoformat_timestamps(node_list)
            file_path = example_sdk.read_node_path(data_model_id)
            file_path.write_text(node_list.dump_yaml())
            typer.echo(f"Downloaded {len(node_list)} nodes to {file_path.relative_to(REPO_ROOT)}")


@app.command("mock", help="Generate mock data for all example SDKs")
def mock(deploy: bool = False):
    client = load_cognite_client_from_toml("config.toml")
    for example_sdk in EXAMPLE_SDKS:
        if not example_sdk.download_nodes:
            typer.echo(f"Skipping {example_sdk.client_name} as it does not download nodes")
            continue
        typer.echo(f"Generating mock data for {example_sdk.client_name}...")
        model = example_sdk.load_data_model()
        dataset = client.data_sets.retrieve(external_id=example_sdk.dataset_external_id)
        if dataset is None:
            raise typer.BadParameter(
                f"Dataset {example_sdk.dataset_external_id} not found. Please deploy it first `cdf deploy`"
            )
        # Special case for Omni were we have a view with external_id "Empty" that should not have mock data
        views = [view for view in model.views if view.external_id != "Empty"]

        generator = MockGenerator(
            views,
            example_sdk.instance_space,
            default_config="faker",
            data_set_id=dataset.id,
            seed=42,
            skip_interfaces=True,
        )
        data = generator.generate_mock_data(node_count=5, max_edge_per_type=3, null_values=0.25)

        data.dump_yaml(DATA_WRITE_DIR, exclude={("Implementation1NonWriteable", "node")})
        typer.echo(f"Generated {len(data.nodes)} nodes and {len(data.edges)} edges for {len(data)} views")
        typer.echo(f"Mock data saved to {DATA_WRITE_DIR.relative_to(REPO_ROOT)}")
        if deploy:
            data.deploy(client, exclude={("Implementation1NonWriteable", "node")}, verbose=True)
            typer.echo("Mock data deployed to CDF")


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


@app.command()
def bump(verbose: bool = False) -> None:
    last_version_str = LAST_VERSION.read_text().strip().removeprefix("v")
    try:
        last_version = parse(last_version_str)
    except ValueError:
        print(f"Invalid last version: {last_version_str}")
        raise SystemExit(1) from None

    bump_text, _ = _read_last_commit_message()
    version_bump = _get_change(bump_text)

    if version_bump == "skip":
        print("No changes to release.")
        return
    if version_bump == "major":
        new_version = Version(f"{last_version.major + 1}.0.0")
    elif version_bump == "minor":
        new_version = Version(f"{last_version.major}.{last_version.minor + 1}.0")
    elif version_bump == "patch":
        new_version = Version(f"{last_version.major}.{last_version.minor}.{last_version.micro + 1}")
    else:
        raise typer.BadParameter("You must specify one of major, minor, patch, alpha, or beta.")

    for file in VERSION_FILES:
        file.write_text(file.read_text().replace(str(VERSION_PLACEHOLDER), str(new_version), 1))
        if verbose:
            typer.echo(f"Bumped version from {last_version} to {new_version} in {file}.")

    typer.echo(f"Bumped version from {last_version} to {new_version} in {len(VERSION_FILES)} files.")


@app.command("changelog")
def create_changelog_entry() -> None:
    bump_text, changelog_text = _read_last_commit_message()
    version_bump = _get_change(bump_text)
    if version_bump == "skip":
        print("No changes to release.")
        return
    if changelog_text is None:
        print(f"No changelog entry found in the last commit message. This is required for a {version_bump} release.")
        raise SystemExit(1)
    _validate_changelog_entry(changelog_text)

    CHANGELOG_ENTRY_FILE.write_text(changelog_text, encoding="utf-8")
    print(f"Changelog entry written to {CHANGELOG_ENTRY_FILE}.")


def _read_last_commit_message() -> tuple[str, str | None]:
    last_git_message = LAST_GIT_MESSAGE_FILE.read_text()
    if "This PR was generated by [Mend Renovate]" in last_git_message:
        print("Skipping Renovate PR.")
        bump_text = "- [ ] Patch\n- [ ] Minor\n- [x] Skip\n"
        return bump_text, None

    if "## Bump" not in last_git_message:
        print("No bump entry found in the last commit message.")
        raise SystemExit(1)

    after_bump = last_git_message.split("## Bump")[1].strip()
    if "## Changelog" not in after_bump:
        return after_bump, None

    bump_text, changelog_text = after_bump.split("## Changelog")

    if "-----" in changelog_text:
        # Co-authors section
        changelog_text = changelog_text.split("-----")[0].strip()

    return bump_text, changelog_text


def _validate_changelog_entry(changelog_text: str) -> None:
    items = [item for item in marko.parse(changelog_text).children if not isinstance(item, marko.block.BlankLine)]
    if not items:
        print("No entries found in the changelog section of the changelog.")
        raise SystemExit(1)
    seen_headers = set()

    last_header: str = ""
    for item in items:
        if isinstance(item, marko.block.Heading):
            if last_header:
                print(f"Expected a list of changes after the {last_header} header.")
                raise SystemExit(1)
            elif item.level != 3:
                print(f"Unexpected header level in changelog: {item}. Should be level 3.")
                raise SystemExit(1)
            elif not isinstance(item.children[0], marko.inline.RawText):
                print(f"Unexpected header in changelog: {item}.")
                raise SystemExit(1)
            header_text = item.children[0].children
            if header_text not in VALID_CHANGELOG_HEADERS:
                print(f"Unexpected header in changelog: {header_text}. Must be one of {VALID_CHANGELOG_HEADERS}.")
                raise SystemExit(1)
            if header_text in seen_headers:
                print(f"Duplicate header in changelog: {header_text}.")
                raise SystemExit(1)
            seen_headers.add(header_text)
            last_header = header_text
        elif isinstance(item, marko.block.List):
            if not last_header:
                print("Expected a header before the list of changes.")
                raise SystemExit(1)
            last_header = ""
        else:
            print(f"Unexpected item in changelog: {item}.")
            raise SystemExit(1)


def _get_change(bump_text: str) -> Literal["major", "minor", "patch", "skip"]:
    items = [item for item in marko.parse(bump_text).children if not isinstance(item, marko.block.BlankLine)]
    if not items:
        print("No items found in the bump section of the commit message.")
        raise SystemExit(1)
    item = items[0]
    if not isinstance(item, marko.block.List):
        print("The first item in the bump must be a list with the type of change.")
        raise SystemExit(1)
    selected: list[Literal["major", "minor", "patch", "skip"]] = []
    for child in item.children:
        if not isinstance(child, marko.block.ListItem):
            print(f"Unexpected item in bump section: {child}")
            raise SystemExit(1)
        if not isinstance(child.children[0], marko.block.Paragraph):
            print(f"Unexpected item in bump section: {child.children[0]}")
            raise SystemExit(1)
        if not isinstance(child.children[0].children[0], marko.inline.RawText):
            print(f"Unexpected item in bump section: {child.children[0].children[0]}")
            raise SystemExit(1)
        list_text = child.children[0].children[0].children
        if list_text.startswith("[ ]"):
            continue
        elif list_text.casefold().startswith("[x]"):
            change_type = list_text[3:].strip()
            if change_type.casefold() not in VALID_BUMP_OPTIONS:
                print(f"Unexpected change type in bump section {change_type}")
                raise SystemExit(1)
            selected.append(change_type.casefold())
        else:
            print(f"Unexpected item in bump section: {list_text}")
            raise SystemExit(1)

    if len(selected) > 1:
        print(f"You can only select one type of change, got {selected}.")
        raise SystemExit(1)
    if not selected:
        print("You must select exactly one type of change, got nothing.")
        raise SystemExit(1)
    return selected[0]


if __name__ == "__main__":
    app()
