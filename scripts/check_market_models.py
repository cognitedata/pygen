from cognite.client import CogniteClient

from movie_domain.client import MovieClient


def main():
    client = MovieClient.from_toml("config.toml")
    c: CogniteClient = client.persons._client

    models = c.data_modeling.data_models.list(space="market", inline_views=True, limit=-1)

    views = c.data_modeling.views.list(space="market", limit=-1, all_versions=True)
    print(models)


if __name__ == "__main__":
    main()
