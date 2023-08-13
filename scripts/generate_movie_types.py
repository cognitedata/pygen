from cognite.pygen._core.dms_to_python import APIGenerator
from cognite.pygen.utils.cdf import load_cognite_client_from_toml
from tests.constants import MovieSDKFiles


def main():
    c = load_cognite_client_from_toml("config.toml")

    views = c.data_modeling.data_models.retrieve(("IntegrationTestsImmutable", "Movie", "2"), inline_views=True)[
        0
    ].views
    for view in views:
        if view.external_id in ["Person", "Actor"]:
            # These classes are manually created and should not be overwritten
            continue
        api_generator = APIGenerator(view, "movie_domain.client")
        data_class = api_generator.generate_data_class_file()

        (MovieSDKFiles.data_classes / f"_{api_generator.class_.file_name}.py").write_text(data_class)
        type_api = api_generator.generate_api_file("movie_domain.client")
        (MovieSDKFiles.api / f"{api_generator.class_.file_name}.py").write_text(type_api)
    print("Done")


if __name__ == "__main__":
    main()
