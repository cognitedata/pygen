from pathlib import Path

repo_root = Path(__file__).resolve().parent.parent

examples_dir = repo_root / "examples"

movie_sdk = examples_dir / "movie_domain"
schemas_dir = repo_root / "tests" / "schemas"


class TestSchemas:
    foobar = schemas_dir / "foobar.graphql"
    case_scenario = schemas_dir / "case_scenario.graphql"


class MovieSDKFiles:
    client_dir = movie_sdk / "client"

    persons_data = client_dir / "data_classes" / "persons.py"
    persons_api = client_dir / "_api" / "persons.py"
    client = client_dir / "_api_client.py"
    client_init = client_dir / "__init__.py"
    data_init = client_dir / "data_classes" / "__init__.py"
    api_init = client_dir / "api_classes" / "__init__.py"
