from __future__ import annotations

import logging
from typing import Dict, Generic, List, Optional, Type, TypeVar, Union, cast, get_args, get_type_hints

import strawberry
from strawberry.experimental.pydantic import UnregisteredTypeException
from strawberry.schema.config import StrawberryConfig

from cognite.pygen.dm_clients.domain_modeling.domain_model import DomainModel
from cognite.pygen.dm_clients.misc import to_snake

from ..custom_types import SCALARS
from ..custom_types._scalars import *  # noqa

DomainModelT = TypeVar("DomainModelT", bound=DomainModel)


logger = logging.getLogger(__name__)
AUTO_GENERATED_COMMENT = """# THIS FILE IS AUTO-GENERATED!
# Use `dm_clients schema render` to update it, see `dm_clients --help` for more information.

"""


class Schema(Generic[DomainModelT]):
    def __init__(self) -> None:
        self.types_map: Dict[str, Type[DomainModelT]] = {}
        self._root_type: Optional[type] = None
        self._root_type_cls: Optional[Type[DomainModelT]] = None
        self._processed_names: List[str] = []

    def _strawberry_schema(self) -> strawberry.Schema:
        if self._root_type is None:
            raise ValueError("define one of the schema types with @schema.register_type(root_type=True)")
        return strawberry.Schema(query=self._root_type, config=StrawberryConfig(auto_camel_case=False))

    def as_str(self) -> str:
        """
        Removing things that DM does not need:
         * `schema { query: ... }`
         * `scalar ...`
        """
        raw_schema = self._strawberry_schema().as_str()
        raw_lines = raw_schema.splitlines()
        # Loop over all the lines *backwards* and remove what we don't need.
        dm_lines: List[str] = []
        for line in raw_lines[::-1]:
            if line.startswith("scalar "):
                if dm_lines:
                    dm_lines.pop()
                continue
            if line.startswith("schema {"):
                dm_lines.pop()
                dm_lines.pop()
                dm_lines.pop()
                continue
            dm_lines.append(line)

        dm_lines.append(AUTO_GENERATED_COMMENT)
        return "\n".join(dm_lines[::-1])

    def register_type(
        self, lowercase_type_name: Optional[Union[str, Type[DomainModelT]]] = None, root_type: bool = False
    ):
        """
        Class decorator for schema types.

        This decorator enables us to use Pydantic models as if Strawberry supported Pydantic models as first-class
        objects.
        There is a pending issue for this, but might be awhile before it's resolved:
        https://github.com/strawberry-graphql/strawberry/issues/2181

        Note: We are still using the Strawberry & Pydantic according to how it is documented in
        https://strawberry.rocks/docs/integrations/pydantic , only with parts of it being dynamic instead of explicit.
        Specifically:
         - Strawberry type classes are dynamically generated here
        """

        def _register_type(cls: Type[DomainModelT]) -> Type[DomainModelT]:
            """Collect a DomainModel for later registration (see `_do_register`)."""
            if not issubclass(cls, DomainModel):
                raise ValueError(
                    f"Classes used with @register_type must inherit from DomainModel, this one does not: {cls}",
                )
            name = cast(str, lowercase_type_name or to_snake(cls.__name__))
            if name in self.types_map:
                raise ValueError(f"Duplicate type in types_map: {name}")
            self.types_map[name] = cls

            if root_type:
                if self._root_type_cls is not None:
                    raise ValueError(f"Multiple types registered with root_type=True: {self._root_type_cls}, {cls}")
                self._root_type_cls = cls

            return cls

        if isinstance(lowercase_type_name, type):
            # decorator was used without attributes, so lowercase_name is actually a DomainModel class
            cls_ = lowercase_type_name
            lowercase_type_name = to_snake(cls_.__name__)
            return _register_type(cls_)

        return _register_type

    def _do_register(self, name: str, cls: Type[DomainModelT]) -> None:
        """Actually register a DomainModel with Strawberry."""

        # this method could be called recursively, so avoid duplicate work:
        if name in self._processed_names:
            return
        self._processed_names.append(name)

        # all field names on our DomainModel:
        field_names = [key for key in cls.__fields__ if key not in {"externalId"}]
        cls_annotations = get_type_hints(cls)

        # find any custom scalar fields (e.g: Timestamp, JSONObject), they need special consideration:
        scalar_fields = {}
        for field_name in field_names:
            field_type = cls_annotations[field_name]
            # find out if there is a custom scalar in this field's annotation:
            while type_args := get_args(field_type):
                field_type = type_args[0]

            type_name = field_type.__name__
            scalar_name = f"{type_name}Scalar"
            # if there is, make a corresponding strawberry scalar (only once), and
            if scalar_name in SCALARS:
                scalar_fields[field_name] = cls.__annotations__[field_name].replace(type_name, scalar_name)

        # dynamically create a strawberry type class:
        cls_dict = {"__annotations__": {field_name: strawberry.auto for field_name in field_names}}
        cls_dict["__annotations__"].update(scalar_fields)
        strawberry_type = type(cls.__name__, (), cls_dict)

        # pass the class to strawberry type decorator:
        try:
            registered_strawberry_type = strawberry.experimental.pydantic.type(model=cls)(strawberry_type)
        except UnregisteredTypeException:
            # Order matters when using `strawberry.experimental.pydantic.type` (unlike `strawberry.type`).
            # It's a trap! https://github.com/strawberry-graphql/strawberry/issues/769
            # So when types are out of order, just run one more `_close()` to work on other types, then
            # try again when it returns. List of names in `self._processed_names` guards against duplicates.
            self._close()
            registered_strawberry_type = strawberry.experimental.pydantic.type(model=cls)(strawberry_type)

        # keep a reference to the schema "root":
        if self._root_type_cls == cls:
            self._root_type = registered_strawberry_type

    def close(self):
        """
        Process all registered types.
        Call this after all DomainModel classes have been decorated with self.register_type.
        """
        self._update_forward_refs()
        self._close()

    def _update_forward_refs(self):
        """Resolve pydantic forward references."""
        for klass in self.types_map.values():
            klass.update_forward_refs()

    def _close(self):
        for name, cls in self.types_map.items():
            self._do_register(name, cls)
