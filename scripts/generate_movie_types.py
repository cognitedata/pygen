from cognite.client import CogniteClient

from cognite import pygen
from cognite.pygen.utils.text import to_snake
from movie_domain.client import MovieClient
from tests.constants import MovieSDKFiles


def main():
    client = MovieClient.from_toml("config.toml")
    c: CogniteClient = client.persons._client

    generator = pygen.SDKGenerator("movie_domain", "Movie")
    # view = c.data_modeling.views.retrieve(("IntegrationTestsImmutable", "Rating"))[0]
    views = c.data_modeling.data_models.retrieve(("IntegrationTestsImmutable", "Movie", "2"), inline_views=True)[
        0
    ].views
    for view in views:
        if view.name in ["Person", "Actor"]:
            # These classes are manually created and should not be overwritten
            continue
        file_name = f"{to_snake(view.name, pluralize=True)}.py"
        data_class = generator.view_to_data_classes(view)

        (MovieSDKFiles.data_classes / f"_{file_name}").write_text(data_class)
        type_api = generator.view_to_api(view)
        (MovieSDKFiles.api / file_name).write_text(type_api)
    print(generator._dependencies_by_view_name)


if __name__ == "__main__":
    main()
