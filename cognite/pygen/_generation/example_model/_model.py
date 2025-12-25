from cognite.pygen._client.models import DataModelRequest, ViewReference

from ._constants import EXTERNAL_ID, SPACE, VERSION

# Create the data model that groups the three views
example_data_model = DataModelRequest(
    space=SPACE,
    external_id=EXTERNAL_ID,
    version=VERSION,
    description="Example data model with product nodes, category nodes, and relationships",
    views=[
        ViewReference(space=SPACE, external_id="ProductNode", version=VERSION),
        ViewReference(space=SPACE, external_id="CategoryNode", version=VERSION),
        ViewReference(space=SPACE, external_id="RelatesTo", version=VERSION),
    ],
)
