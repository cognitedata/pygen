from pathlib import Path

repo_root = Path(__file__).resolve().parent.parent

examples_dir = repo_root / "examples"

movie_sdk = examples_dir / "movie_domain"
schemas_dir = repo_root / "tests" / "schemas"


class TestSchemas:
    foobar = schemas_dir / "foobar.graphql"
    case_scenario = schemas_dir / "case_scenario.graphql"


class DataModels:
    movie_model = schemas_dir / "movie_data_model.yaml"


class MovieSDKFiles:
    client_dir = movie_sdk / "client"

    data_classes = client_dir / "data_classes"
    persons_data = data_classes / "_persons.py"
    actors_data = data_classes / "_actors.py"

    api = client_dir / "_api"
    persons_api = api / "persons.py"
    actors_api = api / "actors.py"

    client = client_dir / "_api_client.py"
    client_init = client_dir / "__init__.py"
    data_init = data_classes / "__init__.py"
    api_init = api / "__init__.py"
