"""

"""

from __future__ import annotations

import itertools
from collections.abc import Iterable
from dataclasses import dataclass

from cognite.client.data_classes import data_modeling as dm
from typing_extensions import Self

from cognite.pygen import config as pygen_config
from cognite.pygen.config.reserved_words import is_reserved_word

from .fields import EdgeOneToOne, Field, PrimitiveField


@dataclass
class FilterParameter:
    name: str
    type_: str
    description: str
    default: str | None = None
    is_nullable: bool = True

    def __post_init__(self):
        if is_reserved_word(self.name, "parameter"):
            self.name = f"{self.name}_"

    @property
    def annotation(self) -> str:
        if self.is_nullable:
            return f"{self.type_} | None"
        else:
            return self.type_

    @property
    def is_time(self) -> bool:
        return self.type_ in ("datetime.datetime", "datetime.date")

    @property
    def is_timestamp(self) -> bool:
        return self.type_ in ("datetime.datetime",)


@dataclass
class FilterCondition:
    filter: type[dm.Filter]
    prop_name: str
    keyword_arguments: dict[str, FilterParameter]
    is_edge_class: bool

    @property
    def condition(self) -> str:
        if self.filter is dm.filters.In:
            parameter_name = next(iter(self.keyword_arguments.values())).name
            return f"{parameter_name} and isinstance({parameter_name}, list)"
        elif self.filter is dm.filters.Equals:
            parameter = next(iter(self.keyword_arguments.values()))
            parameter_type = parameter.type_
            if "|" in parameter_type:
                parameter_type = parameter_type.split("|")[0].strip()
            return f"isinstance({parameter.name}, {parameter_type})"

        return " or ".join(f"{arg.name} is not None" for arg in self.keyword_arguments.values())

    @property
    def arguments(self) -> str:
        if self.prop_name in {"externalId", "space"}:
            instance_type = "edge" if self.is_edge_class else "node"
            property_ref = f'["{instance_type}", "{self.prop_name}"], '
        else:
            property_ref = f'view_id.as_property_ref("{self.prop_name}"), '

        filter_args = self._create_filter_args()

        return f"{property_ref}{', '.join(filter_args)}"

    def _create_filter_args(self) -> list[str]:
        filter_args: list[str] = []
        for keyword, arg in self.keyword_arguments.items():
            if arg.is_time:
                timespec = 'timespec="milliseconds"' if arg.is_timestamp else ""
                filter_args.append(f"{keyword}={arg.name}.isoformat({timespec}) if {arg.name} else None")
            else:
                filter_args.append(f"{keyword}={arg.name}")
        return filter_args

    @property
    def filter_call(self) -> str:
        return f"dm.filters.{self.filter.__name__}"


@dataclass
class FilterConditionOnetoOneEdge(FilterCondition):
    instance_type: type

    @property
    def condition(self) -> str:
        if self.filter is dm.filters.In:
            parameter = next(iter(self.keyword_arguments.values())).name
            return (
                f"{parameter} and isinstance({parameter}, list) and "
                f"isinstance({parameter}[0], {self.instance_type.__name__})"
            )
        elif self.filter is dm.filters.Equals:
            parameter = next(iter(self.keyword_arguments.values())).name
            return f"{parameter} and isinstance({parameter}, {self.instance_type.__name__})"
        raise NotImplementedError(f"Unsupported filter {self.filter} for Direct Relation")

    def _create_filter_args(self) -> list[str]:
        filter_args: list[str] = []
        for keyword, arg in self.keyword_arguments.items():
            if self.instance_type is str and self.filter is dm.filters.Equals:
                filter_args.append(f'{keyword}={{"space": DEFAULT_INSTANCE_SPACE, "externalId": {arg.name}}}')
            elif self.instance_type is tuple and self.filter is dm.filters.Equals:
                filter_args.append(f'{keyword}={{"space": {arg.name}[0], "externalId": {arg.name}[1]}}')
            elif self.instance_type is str and self.filter is dm.filters.In:
                filter_args.append(
                    f'{keyword}=[{{"space": DEFAULT_INSTANCE_SPACE, "externalId": item}} for item in {arg.name}]'
                )
            elif self.instance_type is tuple and self.filter is dm.filters.In:
                filter_args.append(f'{keyword}=[{{"space": item[0], "externalId": item[1]}} for item in {arg.name}]')
            else:
                raise NotImplementedError(f"Unsupported filter {self.filter} for Direct Relation")
        return filter_args


@dataclass
class FilterMethod:
    parameters: list[FilterParameter]
    filters: list[FilterCondition]

    @classmethod
    def from_fields(cls, fields: Iterable[Field], config: pygen_config.Filtering, is_edge_class: bool = False) -> Self:
        parameters_by_name: dict[str, FilterParameter] = {}
        list_filters: list[FilterCondition] = []

        for field_ in itertools.chain(fields, (_EXTERNAL_ID_FIELD, _SPACE_FIELD)):
            # Only primitive and edge one-to-one fields supported for now
            if isinstance(field_, PrimitiveField):
                for selected_filter in config.get(field_.type_, field_.prop_name):
                    if selected_filter is dm.filters.Equals:
                        if field_.name not in parameters_by_name:
                            parameter = FilterParameter(
                                name=field_.name,
                                type_=field_.type_as_string,
                                description=f"The {field_.doc_name} to filter on.",
                            )
                            parameters_by_name[parameter.name] = parameter
                        else:
                            # Equals and In filter share parameter, you have to extend the type hint.
                            parameter = parameters_by_name[field_.name]
                            parameter.type_ = f"{field_.type_as_string} | {parameter.type_}"
                        list_filters.append(
                            FilterCondition(
                                filter=selected_filter,
                                prop_name=field_.prop_name,
                                keyword_arguments=dict(value=parameter),
                                is_edge_class=is_edge_class,
                            )
                        )
                    elif selected_filter is dm.filters.In:
                        if field_.name not in parameters_by_name:
                            parameter = FilterParameter(
                                field_.name,
                                type_=f"list[{field_.type_as_string}]",
                                description=f"The {field_.doc_name} to filter on.",
                            )
                            parameters_by_name[parameter.name] = parameter
                        else:
                            # Equals and In filter share parameter, you have to extend the type hint.
                            parameter = parameters_by_name[field_.name]
                            parameter.type_ = f"{parameter.type_} | list[{field_.type_as_string}]"
                        list_filters.append(
                            FilterCondition(
                                filter=selected_filter,
                                prop_name=field_.prop_name,
                                keyword_arguments=dict(values=parameter),
                                is_edge_class=is_edge_class,
                            )
                        )
                    elif selected_filter is dm.filters.Prefix:
                        parameter = FilterParameter(
                            name=f"{field_.name}_prefix" if field_.name[-1] != "_" else f"{field_.name}prefix",
                            type_=field_.type_as_string,
                            description=f"The prefix of the {field_.doc_name} to filter on.",
                        )
                        parameters_by_name[parameter.name] = parameter
                        list_filters.append(
                            FilterCondition(
                                filter=selected_filter,
                                prop_name=field_.prop_name,
                                keyword_arguments=dict(value=parameter),
                                is_edge_class=is_edge_class,
                            )
                        )
                    elif selected_filter is dm.filters.Range:
                        min_parameter = FilterParameter(
                            name=f"min_{field_.name}",
                            type_=field_.type_as_string,
                            description=f"The minimum value of the {field_.doc_name} to filter on.",
                        )
                        max_parameter = FilterParameter(
                            name=f"max_{field_.name}",
                            type_=field_.type_as_string,
                            description=f"The maximum value of the {field_.doc_name} to filter on.",
                        )
                        parameters_by_name[min_parameter.name] = min_parameter
                        parameters_by_name[max_parameter.name] = max_parameter
                        list_filters.append(
                            FilterCondition(
                                filter=selected_filter,
                                prop_name=field_.prop_name,
                                keyword_arguments=dict(gte=min_parameter, lte=max_parameter),
                                is_edge_class=is_edge_class,
                            )
                        )
                    else:
                        # This is a filter not supported by the list method.
                        continue
            elif isinstance(field_, EdgeOneToOne):
                for selected_filter in config.get(dm.DirectRelation(), field_.prop_name):
                    if selected_filter is dm.filters.Equals:
                        if field_.name not in parameters_by_name:
                            parameter = FilterParameter(
                                name=field_.name,
                                type_="str | tuple[str, str]",
                                description=f"The {field_.doc_name} to filter on.",
                            )
                            parameters_by_name[parameter.name] = parameter
                        else:
                            # Equals and In filter share parameter, you have to extend the type hint.
                            parameter = parameters_by_name[field_.name]
                            parameter.type_ = f"str | tuple[str, str] | {parameter.type_}"
                        list_filters.extend(
                            [
                                FilterConditionOnetoOneEdge(
                                    filter=selected_filter,
                                    prop_name=field_.prop_name,
                                    keyword_arguments=dict(value=parameter),
                                    instance_type=condition_type,
                                    is_edge_class=is_edge_class,
                                )
                                for condition_type in (str, tuple)
                            ]
                        )
                    elif selected_filter is dm.filters.In:
                        if field_.name not in parameters_by_name:
                            parameter = FilterParameter(
                                name=field_.name,
                                type_="list[str] | list[tuple[str, str]]",
                                description=f"The {field_.doc_name} to filter on.",
                            )
                            parameters_by_name[parameter.name] = parameter
                        else:
                            # Equals and In filter share parameter, you have to extend the type hint.
                            parameter = parameters_by_name[field_.name]
                            parameter.type_ = f"{parameter.type_} | list[str] | list[tuple[str, str]]"
                        list_filters.extend(
                            [
                                FilterConditionOnetoOneEdge(
                                    filter=selected_filter,
                                    prop_name=field_.prop_name,
                                    keyword_arguments=dict(values=parameter),
                                    instance_type=condition_type,
                                    is_edge_class=is_edge_class,
                                )
                                for condition_type in (str, tuple)
                            ]
                        )
                    else:
                        # This is a filter not supported.
                        continue

        return cls(parameters=list(parameters_by_name.values()), filters=list_filters)


# These fields are used when creating the list method.
_EXTERNAL_ID_FIELD = PrimitiveField(
    name="external_id",
    prop_name="externalId",
    type_=dm.Text(),
    doc_name="external ID",
    is_nullable=False,
    default=None,
    description=None,
    pydantic_field="Field",
)
_SPACE_FIELD = PrimitiveField(
    name="space",
    prop_name="space",
    type_=dm.Text(),
    doc_name="space",
    is_nullable=False,
    default=None,
    description=None,
    pydantic_field="Field",
)
