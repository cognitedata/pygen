from pathlib import Path

from cognite.client import CogniteClient

from cognite.pygen import generate_sdk


def test_generate_movie_sdk(cognite_client: CogniteClient, tmp_path: Path) -> None:
    # Act/Assert
    generate_sdk(
        ("IntegrationTestsImmutable", "Movie", "2"),
        cognite_client,
        output_dir=tmp_path,
    )


def test_generate_shop_sdk(cognite_client: CogniteClient, tmp_path: Path) -> None:
    generate_sdk(
        ("IntegrationTestsImmutable", "SHOP_Model", "2"),
        cognite_client,
        output_dir=tmp_path,
    )
