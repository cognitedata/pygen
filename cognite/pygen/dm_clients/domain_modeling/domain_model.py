from __future__ import annotations

import logging
from typing import Dict, Optional, Type, get_args

import strawberry
from pydantic import Extra, PrivateAttr
from typing_extensions import Self

from cognite.pygen.dm_clients.cdf.data_classes_dm_v3 import DataModelBase

__all__ = [
    "DomainModel",
]


logger = logging.getLogger(__name__)


class DomainModel(DataModelBase):
    """
    Base class for all models in schema_types.py

    A bit of nomenclature: instances of DomainModel we call "items".
    This is pretty much because all other words I can think of have been taken by DM >:]

    Even this is confusing... A data model in DM is a collection of domain models.
    """

    externalId: strawberry.Private[Optional[str]] = None
    _reference: strawberry.Private[bool] = PrivateAttr(False)
    # strawberry.Private ^ means the field will not be exposed in GraphQL schema
    # Used on externalID because actual objects (returned from the API) have these fields. These are "implicit" field.
    # PrivateAttr is telling pydantic to allow the use of this as a regular (non-pydantic) attribute.

    class Config:
        extra = Extra.forbid
        #  ^ raises exception if extra fields are passed to the constructor. Very useful for development.
        arbitrary_types_allowed = True

    def __init__(self, *args, _reference: bool = False, **kwargs):
        if _reference:
            for field_name, field in self.__fields__.items():
                if field.required and field_name not in kwargs:
                    kwargs[field_name] = field.type_()
        super().__init__(*args, **kwargs)
        self._reference = _reference

    def __repr_args__(self):
        if self._reference:
            return [(key, getattr(self, key)) for key in ("externalId", "_reference")]
        else:
            return super().__repr_args__()

    def as_reference(self) -> Self:
        return type(self)(externalId=self.externalId, _reference=True)

    @classmethod
    def ref(cls, externalId: str):  # noqa: N803
        return cls(externalId=externalId, _reference=True)

    @classmethod
    def get_one_to_many_attrs(cls) -> Dict[str, Type[DomainModel]]:
        """
        Get attributes which describe one-to-many relationships.
        These attributes usually require additional considerations with DM.
        """
        attrs = {}
        props: Dict[str, dict] = cls.schema()["properties"]
        for field_name, field_info in props.items():
            if field_info.get("type") != "array":
                continue  # one-to-many has to be an array
            if field_info.get("items", {}).get("$ref", "").startswith("#/definitions/"):
                # TODO assuming that the reference is local, probably ok.
                field_type = cls.get_type_for_attr(field_name)
                attrs[field_name] = field_type
        return attrs

    @classmethod
    def get_one_to_one_attrs(cls) -> Dict[str, Type[DomainModel]]:
        """Get attributes which describe one-to-one relationships."""
        attrs = {}
        props: Dict[str, dict] = cls.schema()["properties"]
        for field_name, field_info in props.items():
            if field_info.get("type") == "array":
                continue  # one-to-one cannot be an array
            if field_info.get("$ref", "").startswith("#/definitions/"):
                # TODO assuming that the reference is local, probably ok.
                field_type = cls.get_type_for_attr(field_name)
                attrs[field_name] = field_type
        return attrs

    @classmethod
    def get_type_for_attr(cls, attr: str) -> Type[DomainModel]:
        """Given an attribute, return a DomainModel subclass to which the attribute refers to."""
        field_type = cls.__fields__[attr].type_
        # strip annotations (like List[] and Optional[]):
        while type_args := get_args(field_type):
            field_type = type_args[0]
        if issubclass(field_type, DomainModel):
            return field_type
        raise ValueError(f"Attr {attr} not a reference to DomainModel.")
