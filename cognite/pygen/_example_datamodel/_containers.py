from cognite.pygen._client.models import (
    BooleanProperty,
    ContainerPropertyDefinition,
    ContainerRequest,
    DateProperty,
    DirectNodeRelation,
    Float32Property,
    Float64Property,
    Int32Property,
    TextProperty,
    TimestampProperty,
)

from ._constants import SPACE

# Container for ProductNode view
product_container = ContainerRequest(
    space=SPACE,
    external_id="Product",
    name="Product",
    description="Container for product data",
    used_for="node",
    properties={
        "name": ContainerPropertyDefinition(
            type=TextProperty(),
            nullable=False,
            name="Product Name",
        ),
        "description": ContainerPropertyDefinition(
            type=TextProperty(),
            nullable=True,
            name="Product Description",
        ),
        "tags": ContainerPropertyDefinition(
            type=TextProperty(list=True),
            nullable=True,
            name="Tags",
        ),
        "price": ContainerPropertyDefinition(
            type=Float64Property(),
            nullable=False,
            name="Price",
        ),
        "prices": ContainerPropertyDefinition(
            type=Float64Property(list=True),
            nullable=True,
            name="Historical Prices",
        ),
        "quantity": ContainerPropertyDefinition(
            type=Int32Property(),
            nullable=False,
            name="Quantity in Stock",
        ),
        "quantities": ContainerPropertyDefinition(
            type=Int32Property(list=True),
            nullable=True,
            name="Historical Quantities",
        ),
        "active": ContainerPropertyDefinition(
            type=BooleanProperty(),
            nullable=True,
            name="Is Active",
        ),
        "created_date": ContainerPropertyDefinition(
            type=DateProperty(),
            nullable=False,
            name="Created Date",
        ),
        "updated_timestamp": ContainerPropertyDefinition(
            type=TimestampProperty(),
            nullable=True,
            name="Last Updated Timestamp",
        ),
        "category": ContainerPropertyDefinition(
            type=DirectNodeRelation(),
            nullable=True,
            name="Category Relation",
        ),
    },
)

# Container for CategoryNode view
category_container = ContainerRequest(
    space=SPACE,
    external_id="Category",
    name="Category",
    description="Container for category data",
    used_for="node",
    properties={
        "category_name": ContainerPropertyDefinition(
            type=TextProperty(),
            nullable=False,
            name="Category Name",
        ),
    },
)

# Container for RelatesTo edge view
relates_to_container = ContainerRequest(
    space=SPACE,
    external_id="RelatesTo",
    name="RelatesTo",
    description="Container for relationship data",
    used_for="edge",
    properties={
        "relation_type": ContainerPropertyDefinition(
            type=TextProperty(),
            nullable=False,
            name="Relation Type",
        ),
        "strength": ContainerPropertyDefinition(
            type=Float32Property(),
            nullable=True,
            name="Relation Strength",
        ),
        "created_at": ContainerPropertyDefinition(
            type=TimestampProperty(),
            nullable=False,
            name="Relation Created At",
        ),
    },
)
