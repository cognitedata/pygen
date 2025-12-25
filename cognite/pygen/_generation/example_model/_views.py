from cognite.pygen._client.models import (
    ContainerDirectReference,
    ContainerReference,
    MultiReverseDirectRelationPropertyRequest,
    ViewCorePropertyRequest,
    ViewReference,
    ViewRequest,
)

from ._constants import SPACE, VERSION

# ProductNode view
product_view = ViewRequest(
    space=SPACE,
    external_id="ProductNode",
    version=VERSION,
    name="Product Node",
    description="View for product nodes with various data types and relations",
    filter=None,
    properties={
        "name": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Product"),
            container_property_identifier="name",
            name="Product Name",
        ),
        "description": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Product"),
            container_property_identifier="description",
            name="Product Description",
        ),
        "tags": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Product"),
            container_property_identifier="tags",
            name="Tags",
        ),
        "price": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Product"),
            container_property_identifier="price",
            name="Price",
        ),
        "prices": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Product"),
            container_property_identifier="prices",
            name="Historical Prices",
        ),
        "quantity": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Product"),
            container_property_identifier="quantity",
            name="Quantity in Stock",
        ),
        "quantities": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Product"),
            container_property_identifier="quantities",
            name="Historical Quantities",
        ),
        "active": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Product"),
            container_property_identifier="active",
            name="Is Active",
        ),
        "created_date": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Product"),
            container_property_identifier="created_date",
            name="Created Date",
        ),
        "updated_timestamp": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Product"),
            container_property_identifier="updated_timestamp",
            name="Last Updated Timestamp",
        ),
        "category": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Product"),
            container_property_identifier="category",
            name="Category Relation",
            source=ViewReference(space=SPACE, external_id="CategoryNode", version=VERSION),
        ),
    },
)

# CategoryNode view
category_view = ViewRequest(
    space=SPACE,
    external_id="CategoryNode",
    version=VERSION,
    name="Category Node",
    description="View for category nodes with reverse direct relation",
    filter=None,
    properties={
        "category_name": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="Category"),
            container_property_identifier="category_name",
            name="Category Name",
        ),
        "products": MultiReverseDirectRelationPropertyRequest(
            source=ViewReference(space=SPACE, external_id="ProductNode", version=VERSION),
            through=ContainerDirectReference(
                source=ContainerReference(space=SPACE, external_id="Product"),
                identifier="category",
            ),
            name="Products in this Category",
        ),
    },
)

# RelatesTo edge view
relates_to_view = ViewRequest(
    space=SPACE,
    external_id="RelatesTo",
    version=VERSION,
    name="RelatesTo Edge",
    description="Edge view for relating products or other nodes",
    filter=None,
    properties={
        "relation_type": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="RelatesTo"),
            container_property_identifier="relation_type",
            name="Relation Type",
        ),
        "strength": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="RelatesTo"),
            container_property_identifier="strength",
            name="Relation Strength",
        ),
        "created_at": ViewCorePropertyRequest(
            container=ContainerReference(space=SPACE, external_id="RelatesTo"),
            container_property_identifier="created_at",
            name="Relation Created At",
        ),
    },
)
