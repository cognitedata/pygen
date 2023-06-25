from cognite.client import CogniteClient

from cognite import pygen
from cognite.pygen.utils.text import to_snake
from movie_domain.client import MovieClient
from tests.constants import MovieSDKFiles


def main():
    client = MovieClient.from_toml("config.toml")
    c: CogniteClient = client.persons._client

    view = c.data_modeling.views.retrieve(("IntegrationTestsImmutable", "Actor"))[0]
    file_name = f"{to_snake(view.name, pluralize=True)}.py"
    data_class = pygen.view_to_data_classes(view)

    (MovieSDKFiles.data_classes / file_name).write_text(data_class)
    type_api = pygen.view_to_api(view, sdk_name="movie_domain")
    (MovieSDKFiles.api / file_name).write_text(type_api)


if __name__ == "__main__":
    main()
