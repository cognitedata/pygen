"""
Helper script to create views from containers for the MappedPropertyApply objects.
(Faster than creating them manually)
"""

from pathlib import Path

from cognite.client import data_modeling as dm

VIEW_DIR = Path(__file__).resolve().parent
CONTAINER_DIR = VIEW_DIR.parent / "containers"


def main():
    container_filepaths = list(CONTAINER_DIR.glob("*.container.yaml"))
    for filepath in container_filepaths:
        if filepath.stem.startswith("Connection") or filepath.stem.startswith("Polymorphism"):
            continue
        container = dm.ContainerApply.load(filepath.read_text())
        view = dm.ViewApply(
            space=container.space,
            external_id=container.external_id,
            name=container.name,
            description=container.description,
            version="1",
            properties={
                identifier: dm.MappedPropertyApply(
                    container=container.as_id(),
                    container_property_identifier=identifier,
                    name=prop.name,
                    description=prop.description,
                )
                for identifier, prop in container.properties.items()
            },
        )
        view_filepath = VIEW_DIR / f"{view.external_id}.view.yaml"
        view_filepath.write_text(view.dump_yaml())


if __name__ == "__main__":
    main()
