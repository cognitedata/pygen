from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from movie_domain_pydantic_v1.client.data_classes import (
    Person,
    PersonApply,
    PersonList,
    PersonApplyList,
    PersonFields,
    PersonTextFields,
    DomainModelApply,
)
from movie_domain_pydantic_v1.client.data_classes._person import _PERSON_PROPERTIES_BY_FIELD


class PersonRolesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        """Retrieve one or more roles edges by id(s) of a person.

        Args:
            external_id: External id or list of external ids source person.
            space: The space where all the role edges are located.

        Returns:
            The requested role edges.

        Examples:

            Retrieve roles edge by id:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> person = client.person.roles.retrieve("my_roles")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Person.roles"},
        )
        if isinstance(external_id, str):
            is_person = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_person))

        else:
            is_persons = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_persons))

    def list(
        self, person_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """List roles edges of a person.

        Args:
            person_id: Id of the source person.
            limit: Maximum number of role edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the role edges are located.

        Returns:
            The requested role edges.

        Examples:

            List 5 roles edges connected to "my_person":

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> person = client.person.roles.list("my_person", limit=5)

        """
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Person.roles"},
        )
        filters.append(is_edge_type)
        if person_id:
            person_ids = [person_id] if isinstance(person_id, str) else person_id
            is_persons = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in person_ids],
            )
            filters.append(is_persons)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class PersonAPI(TypeAPI[Person, PersonApply, PersonList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[PersonApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Person,
            class_apply_type=PersonApply,
            class_list=PersonList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.roles = PersonRolesAPI(client)

    def apply(self, person: PersonApply | Sequence[PersonApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) persons.

        Note: This method iterates through all nodes linked to person and create them including the edges
        between the nodes. For example, if any of `roles` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            person: Person or sequence of persons to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new person:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> from movie_domain_pydantic_v1.client.data_classes import PersonApply
                >>> client = MovieClient()
                >>> person = PersonApply(external_id="my_person", ...)
                >>> result = client.person.apply(person)

        """
        if isinstance(person, PersonApply):
            instances = person.to_instances_apply(self._view_by_write_class)
        else:
            instances = PersonApplyList(person).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more person.

        Args:
            external_id: External id of the person to delete.
            space: The space where all the person are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete person by id:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> client.person.delete("my_person")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Person:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PersonList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> Person | PersonList:
        """Retrieve one or more persons by id(s).

        Args:
            external_id: External id or list of external ids of the persons.
            space: The space where all the persons are located.

        Returns:
            The requested persons.

        Examples:

            Retrieve person by id:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> person = client.person.retrieve("my_person")

        """
        if isinstance(external_id, str):
            person = self._retrieve((space, external_id))

            role_edges = self.roles.retrieve(external_id)
            person.roles = [edge.end_node.external_id for edge in role_edges]

            return person
        else:
            persons = self._retrieve([(space, ext_id) for ext_id in external_id])

            role_edges = self.roles.retrieve(external_id)
            self._set_roles(persons, role_edges)

            return persons

    def search(
        self,
        query: str,
        properties: PersonTextFields | Sequence[PersonTextFields] | None = None,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PersonList:
        """Search persons

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_birth_year: The minimum value of the birth year to filter on.
            max_birth_year: The maximum value of the birth year to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of persons to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `roles` external ids for the persons. Defaults to True.

        Returns:
            Search results persons matching the query.

        Examples:

           Search for 'my_person' in all text properties:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> persons = client.person.search('my_person')

        """
        filter_ = _create_filter(
            self._view_id,
            min_birth_year,
            max_birth_year,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _PERSON_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PersonFields | Sequence[PersonFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PersonTextFields | Sequence[PersonTextFields] | None = None,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PersonFields | Sequence[PersonFields] | None = None,
        group_by: PersonFields | Sequence[PersonFields] = None,
        query: str | None = None,
        search_properties: PersonTextFields | Sequence[PersonTextFields] | None = None,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PersonFields | Sequence[PersonFields] | None = None,
        group_by: PersonFields | Sequence[PersonFields] | None = None,
        query: str | None = None,
        search_property: PersonTextFields | Sequence[PersonTextFields] | None = None,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across persons

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_birth_year: The minimum value of the birth year to filter on.
            max_birth_year: The maximum value of the birth year to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of persons to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `roles` external ids for the persons. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count persons in space `my_space`:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.person.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            min_birth_year,
            max_birth_year,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PERSON_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PersonFields,
        interval: float,
        query: str | None = None,
        search_property: PersonTextFields | Sequence[PersonTextFields] | None = None,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for persons

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_birth_year: The minimum value of the birth year to filter on.
            max_birth_year: The maximum value of the birth year to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of persons to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `roles` external ids for the persons. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            min_birth_year,
            max_birth_year,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PERSON_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> PersonList:
        """List/filter persons

        Args:
            min_birth_year: The minimum value of the birth year to filter on.
            max_birth_year: The maximum value of the birth year to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of persons to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `roles` external ids for the persons. Defaults to True.

        Returns:
            List of requested persons

        Examples:

            List persons and limit to 5:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> persons = client.person.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            min_birth_year,
            max_birth_year,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )

        persons = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := persons.as_external_ids()) > IN_FILTER_LIMIT:
                role_edges = self.roles.list(limit=-1)
            else:
                role_edges = self.roles.list(external_ids, limit=-1)
            self._set_roles(persons, role_edges)

        return persons

    @staticmethod
    def _set_roles(persons: Sequence[Person], role_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in role_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for person in persons:
            node_id = person.id_tuple()
            if node_id in edges_by_start_node:
                person.roles = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    min_birth_year: int | None = None,
    max_birth_year: int | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_birth_year or max_birth_year:
        filters.append(dm.filters.Range(view_id.as_property_ref("birthYear"), gte=min_birth_year, lte=max_birth_year))
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
