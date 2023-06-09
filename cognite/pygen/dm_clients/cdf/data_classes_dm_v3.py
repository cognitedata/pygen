from __future__ import annotations

from contextlib import suppress
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConstrainedStr, validator


class DataModelBase(BaseModel):
    class Config:
        # alias_generator = to_camel
        allow_population_by_field_name = True


class Property(DataModelBase):
    name: Optional[str] = None
    nullable: Optional[bool] = None
    autoIncrement: Optional[bool] = None
    defaultValue: Optional[Union[str, int, float, bool, dict]] = None
    description: Optional[str] = None
    type: dict  # TODO much structure here


class Space(DataModelBase):
    space: str
    name: Optional[str] = None
    description: Optional[str] = None


class View(DataModelBase):
    space: str
    externalId: str
    version: str
    name: Optional[str] = None
    description: Optional[str] = None
    filter: Optional[dict] = None
    implements: Optional[List[dict]] = None
    properties: Optional[dict] = None


class DataModel(DataModelBase):
    space: str
    externalId: str
    version: str
    name: str = ""
    description: str = ""
    views: List[View]


class Container(DataModelBase):
    space: str
    externalId: str
    properties: Dict[str, Property]
    usedFor: Literal["node", "edge", "all"] = "node"
    constraints: Optional[dict] = None
    indexes: Optional[dict] = None


class Node(DataModelBase):
    instanceType: Literal["node"] = "node"
    space: str
    externalId: str
    version: Optional[str] = None
    createdTime: Optional[int] = None
    lastUpdatedTime: Optional[int] = None
    deletedTime: Optional[int] = None
    properties: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None
    # TODO ^ much structure here, nested spaces, view-or-container, actual props dict

    def get_properties(self, view: View) -> Dict[str, Any]:
        """Simplification, assumes that nodes have all properties in just one space and one view."""
        return (self.properties or {}).get(self.space, {}).get(view.externalId, {})

    def update_properties(self, view: View, props_update: Dict[str, Any]):
        props = self.get_properties(view)
        props.update(props_update)
        self.properties = {self.space: {view.externalId: props}}

    @validator("properties", pre=True)
    def discard_view_version_in_props(cls, value: dict) -> dict:
        """
        The API endpoints `retrieve` and `list` return nodes with props nested under "{view.externalID}/{view.version}"
        (which is in turn nested under space_id).
        This validator strips the "/{view.version}" part of that key. This makes it the same format which API expects
        on the `apply` endpoint. This makes working with Nodes a bit simpler.
        """
        with suppress(IndexError, KeyError, ValueError, TypeError):
            space = list(value.keys())[0]
            key = list(value[space].keys())[0]
            ext_id, _version = key.split("/")
            props = value[space][key]
            del value[space][key]
            value[space][ext_id] = props
        return value


class RelationReference(DataModelBase):
    space: str
    externalId: str


class Edge(DataModelBase):
    instanceType: Literal["edge"] = "edge"
    space: str
    externalId: str
    type: RelationReference
    startNode: RelationReference
    endNode: RelationReference
    version: Optional[str] = None
    createdTime: Optional[int] = None
    lastUpdatedTime: Optional[int] = None
    deletedTime: Optional[int] = None
    properties: Optional[dict] = None  # TODO much structure here, nested spaces, view-or-container, then properties
    # TODO Edge.properties not fully supported yet by DM and not implemented in this library!


class ExternalIdStr(ConstrainedStr):
    min_length = 1
    max_length = 255


# TODO make use of ExternalIdStr ^ ?
