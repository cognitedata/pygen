"""This module contains the class representing the filter used in the generated classes."""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen import config as pygen_config
from cognite.pygen.config.reserved_words import is_reserved_word

from .fields import BaseConnectionField, Field, OneToManyConnectionField, OneToOneConnectionField, PrimitiveField


@dataclass
class FilterParameter:
    """This class represents a single parameter in a filter method.

    Args:
        name: The name of the parameter.
        type_: The type of the parameter.
        description: The description of the parameter.
        default: The default value of the parameter.
        is_nullable: Whether the parameter is nullable.
    """

    name: str
    type_: str
    description: str
    default: str | None = None
    is_nullable: bool = True

    def __post_init__(self):
        if is_reserved_word(self.name, "parameter"):
            self.name = f"{self.name}_"

    def copy(self, name: str) -> FilterParameter:
        return FilterParameter(
            name=name,
            type_=self.type_,
            description=self.description,
            default=self.default,
            is_nullable=self.is_nullable,
        )

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
class FilterImplementation:
    """This class represent a filter implementation.

    It is used in the methods that require filtering to create the DMS filter.

    Args:
        filter: The DMS filter to use.
        prop_name: The name of the property in the View to filter on.
        keyword_arguments: The keyword arguments of the filter.
        is_edge_class: Whether the class is an edge class.
    """

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
class FilterImplementationOnetoOneEdge(FilterImplementation):
    instance_type: str
    has_default_instance_space: bool

    @property
    def condition(self) -> str:
        if self.filter is dm.filters.In:
            parameter = next(iter(self.keyword_arguments.values())).name
            return (
                f"{parameter} and isinstance({parameter}, Sequence) "
                f"and not isinstance({parameter}, str) and not is_tuple_id({parameter})"
            )
        elif self.filter is dm.filters.Equals:
            parameter = next(iter(self.keyword_arguments.values())).name
            return f"isinstance({parameter}, {self.instance_type}) or is_tuple_id({parameter})"
        raise NotImplementedError(f"Unsupported filter {self.filter} for Direct Relation")

    def _create_filter_args(self) -> list[str]:
        filter_args: list[str] = []
        for keyword, arg in self.keyword_arguments.items():
            if self.instance_type == Sequence.__name__ and self.filter is dm.filters.In:
                filter_args.append(f"{keyword}=[as_instance_dict_id(item) for item in {arg.name}]")
            else:
                filter_args.append(f"{keyword}=as_instance_dict_id({arg.name})")
        return filter_args


@dataclass
class FilterMethod:
    """This classe represents a filter that can be used in any method that requires filtering.

    Args:
        parameters: The parameters of the filter.
        implementations: The conditions of the filter.

    """

    parameters: list[FilterParameter]
    implementations: list[FilterImplementation]

    @classmethod
    def from_fields(
        cls,
        fields: Iterable[Field],
        config: pygen_config.Filtering,
        has_default_instance_space: bool,
        is_edge_class: bool = False,
    ) -> FilterMethod:
        parameters_by_name: dict[str, FilterParameter] = {}
        list_filters: list[FilterImplementation] = []

        for field_ in itertools.chain(fields, (_EXTERNAL_ID_FIELD, _SPACE_FIELD)):
            if isinstance(field_, BaseConnectionField) and field_.is_reverse_direct_relation:
                # Reverse direct relations are not supported in list methods.
                continue
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
                            FilterImplementation(
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
                            FilterImplementation(
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
                            FilterImplementation(
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
                            FilterImplementation(
                                filter=selected_filter,
                                prop_name=field_.prop_name,
                                keyword_arguments=dict(gte=min_parameter, lte=max_parameter),
                                is_edge_class=is_edge_class,
                            )
                        )
                    else:
                        # This is a filter not supported by the list method.
                        continue
            elif isinstance(field_, OneToOneConnectionField | OneToManyConnectionField) and field_.is_direct_relation:
                for selected_filter in config.get(dm.DirectRelation(), field_.prop_name):
                    if selected_filter is dm.filters.Equals:
                        type_ = "tuple[str, str] | dm.NodeId | dm.DirectRelationReference"
                        if has_default_instance_space:
                            type_ = f"str | {type_}"
                        if field_.name not in parameters_by_name:
                            parameter = FilterParameter(
                                name=field_.name,
                                type_=type_,
                                description=f"The {field_.doc_name} to filter on.",
                            )
                            parameters_by_name[parameter.name] = parameter
                        else:
                            # Equals and In filter share parameter, you have to extend the type hint.
                            parameter = parameters_by_name[field_.name]
                            parameter.type_ = f"{type_} | {parameter.type_}"
                        instance_type = "dm.NodeId | dm.DirectRelationReference"
                        if has_default_instance_space:
                            instance_type = f"str | {instance_type}"

                        list_filters.append(
                            FilterImplementationOnetoOneEdge(
                                filter=selected_filter,
                                prop_name=field_.prop_name,
                                keyword_arguments=dict(value=parameter),
                                instance_type=instance_type,
                                is_edge_class=is_edge_class,
                                has_default_instance_space=has_default_instance_space,
                            )
                        )
                    elif selected_filter is dm.filters.In:
                        type_ = "tuple[str, str] | dm.NodeId | dm.DirectRelationReference"
                        if has_default_instance_space:
                            type_ = f"str | {type_}"
                        type_ = f"Sequence[{type_}]"

                        if field_.name not in parameters_by_name:
                            parameter = FilterParameter(
                                name=field_.name,
                                type_=type_,
                                description=f"The {field_.doc_name} to filter on.",
                            )
                            parameters_by_name[parameter.name] = parameter
                        else:
                            # Equals and In filter share parameter, you have to extend the type hint.
                            parameter = parameters_by_name[field_.name]
                            parameter.type_ = f"{parameter.type_} | {type_}"
                        list_filters.append(
                            FilterImplementationOnetoOneEdge(
                                filter=selected_filter,
                                prop_name=field_.prop_name,
                                keyword_arguments=dict(values=parameter),
                                instance_type=Sequence.__name__,
                                is_edge_class=is_edge_class,
                                has_default_instance_space=has_default_instance_space,
                            )
                        )
                    else:
                        # This is a filter not supported.
                        continue

        return cls(parameters=list(parameters_by_name.values()), implementations=list_filters)


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
