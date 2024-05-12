from __future__ import annotations

from dataclasses import dataclass

from cognite.client.data_classes import data_modeling as dm

from .primitive import ListFieldCore, PrimitiveFieldCore


@dataclass(frozen=True)
class CDFExternalField(PrimitiveFieldCore):
    @property
    def is_time_series(self) -> bool:
        return isinstance(self.type_, dm.TimeSeriesReference)

    @property
    def is_list(self) -> bool:
        return self.type_.is_list

    def as_read_type_hint(self) -> str:
        return self.as_write_type_hint()

    def as_graphql_type_hint(self) -> str:
        type_ = self.type_as_string
        if type_ != "str":
            type_ = f"{type_}, dict"
        else:
            # GraphQL returns dict for CDF External Fields
            type_ = "dict"

        # CDF External Fields are always nullable
        if self.need_alias:
            out_type = f'Union[{type_}, None] = {self.pydantic_field}(None, alias="{self.prop_name}")'
        else:
            out_type = f"Union[{type_}, None] = None"
        return out_type

    def as_write_type_hint(self) -> str:
        type_ = self.type_as_string
        if type_ != "str":
            type_ = f"{type_}, str"

        # CDF External Fields are always nullable
        if self.need_alias:
            out_type = f'Union[{type_}, None] = {self.pydantic_field}(None, alias="{self.prop_name}")'
        else:
            out_type = f"Union[{type_}, None] = None"
        return out_type

    def as_write_graphql(self) -> str:
        if self.is_time_series:
            # TimeSeries Supports converting from dict.
            return self.as_write()

        if self.is_list:
            return f'[item["externalId"] for item in self.{self.name} or [] if "externalId" in item] or None'
        else:
            return f'self.{self.name}["externalId"] if self.{self.name} and "externalId" in self.{self.name} else None'

    def as_read(self) -> str:
        # Read is only used in graphql
        return self.as_write_graphql()


@dataclass(frozen=True)
class CDFExternalListField(ListFieldCore, CDFExternalField):
    """
    This represents a list of CDF types such as list[TimeSeries], list[Sequence].
    """

    def as_read_type_hint(self) -> str:
        type = self.type_as_string
        if type != "str":
            if self.need_alias:
                return (
                    f"Union[list[{self.type_as_string}], list[str], None] = "
                    f'{self.pydantic_field}(None, alias="{self.prop_name}")'
                )
            else:
                return f"Union[list[{self.type_as_string}], list[str], None] = None"
        else:
            if self.need_alias:
                return f'Optional[list[str]] = {self.pydantic_field}(None, alias="{self.prop_name}")'
            else:
                return "Optional[list[str]] = None"

    def as_write_type_hint(self) -> str:
        type_ = self.type_as_string
        if type_ != "str":
            if self.is_nullable and self.need_alias:
                return f'Union[list[{type_}], list[str], None] = {self.pydantic_field}(None, alias="{self.prop_name}")'
            elif self.need_alias:
                return f'Union[list[{type_}], list[str]] = {self.pydantic_field}(alias="{self.prop_name}")'
            elif self.is_nullable:
                return f"Union[list[{type_}], list[str], None] = None"
            else:  # not self.is_nullable and not self.need_alias
                return f"Union[list[{type_}], list[str]]"
        else:
            if self.is_nullable and self.need_alias:
                return f'Optional[list[str]] = {self.pydantic_field}(None, alias="{self.prop_name}")'
            elif self.need_alias:
                return f'list[str] = {self.pydantic_field}(alias="{self.prop_name}")'
            elif self.is_nullable:
                return "Optional[list[str]] = None"
            else:
                return "list[str]"

    def as_graphql_type_hint(self) -> str:
        type_ = self.type_as_string
        if type_ != "str":
            if self.is_nullable and self.need_alias:
                return f'Union[list[{type_}], list[dict], None] = {self.pydantic_field}(None, alias="{self.prop_name}")'
            elif self.need_alias:
                return f'Union[list[{type_}], list[dict]] = {self.pydantic_field}(alias="{self.prop_name}")'
            elif self.is_nullable:
                return f"Union[list[{type_}], list[dict], None] = None"
            else:
                return f"Union[list[{type_}], list[dict]]"
        else:
            if self.is_nullable and self.need_alias:
                return f'Optional[list[dict]] = {self.pydantic_field}(None, alias="{self.prop_name}")'
            elif self.need_alias:
                return f'list[dict] = {self.pydantic_field}(alias="{self.prop_name}")'
            elif self.is_nullable:
                return "Optional[list[dict]] = None"
            else:
                return "list[dict]"
