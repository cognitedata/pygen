from pathlib import Path

repo_root = Path(__file__).resolve().parent.parent

examples_dir = repo_root / "examples"
examples_dir_pydantic_v1 = repo_root / "examples-pydantic-v1"
movie_sdk = examples_dir / "movie_domain"
schemas_dir = repo_root / "tests" / "schemas"
shop_sdk = examples_dir / "shop"
market_sdk = examples_dir / "markets"


class TestSchemas:
    foobar = schemas_dir / "foobar.graphql"
    case_scenario = schemas_dir / "case_scenario.graphql"


class DataModels:
    movie_model = schemas_dir / "movie_data_model.yaml"
    shop_model = schemas_dir / "shop_data_model.yaml"
    cog_pool = schemas_dir / "CogPool.yaml"
    pygen_pool = schemas_dir / "PygenPool.yaml"


class MarketSDKFiles:
    client_dir = market_sdk / "client"
    client = client_dir / "_api_client.py"
    date_transformation_pair_data = client_dir / "data_classes" / "_date_transformation_pairs.py"
    date_transformation_pair_api = client_dir / "_api" / "date_transformation_pairs.py"


class ShopSDKFiles:
    client_dir = shop_sdk / "client"
    data_classes = client_dir / "data_classes"
    api = client_dir / "_api"
    cases_data = data_classes / "_cases.py"
    command_configs_data = data_classes / "_command_configs.py"
    data_init = data_classes / "__init__.py"
    command_configs_api = api / "command_configs.py"


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
