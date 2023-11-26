from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from movie_domain.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Person,
    PersonApply,
    PersonFields,
    PersonList,
    PersonTextFields,
)
from movie_domain.client.data_classes._person import (
    _PERSON_PROPERTIES_BY_FIELD,
    _create_person_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .person_roles import PersonRolesAPI
from .person_query import PersonQueryAPI


class PersonAPI(NodeAPI[Person, PersonApply, PersonList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[PersonApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Person,
            class_apply_type=PersonApply,
            class_list=PersonList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.roles_edge = PersonRolesAPI(client)

    def __call__(
        self,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PersonQueryAPI[PersonList]:
        """Query starting at persons.

        Args:
            min_birth_year: The minimum value of the birth year to filter on.
            max_birth_year: The maximum value of the birth year to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of persons to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for persons.

        """
        filter_ = _create_person_filter(
            self._view_id,
            min_birth_year,
            max_birth_year,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            PersonList,
            [
                QueryStep(
                    name="person",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_PERSON_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Person,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return PersonQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, person: PersonApply | Sequence[PersonApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) persons.

        Note: This method iterates through all nodes and timeseries linked to person and creates them including the edges
        between the nodes. For example, if any of `roles` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            person: Person or sequence of persons to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new person:

                >>> from movie_domain.client import MovieClient
                >>> from movie_domain.client.data_classes import PersonApply
                >>> client = MovieClient()
                >>> person = PersonApply(external_id="my_person", ...)
                >>> result = client.person.apply(person)

        """
        return self._apply(person, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more person.

        Args:
            external_id: External id of the person to delete.
            space: The space where all the person are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete person by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> client.person.delete("my_person")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Person:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> PersonList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> Person | PersonList:
        """Retrieve one or more persons by id(s).

        Args:
            external_id: External id or list of external ids of the persons.
            space: The space where all the persons are located.

        Returns:
            The requested persons.

        Examples:

            Retrieve person by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> person = client.person.retrieve("my_person")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_pairs=[
                (self.roles_edge, "roles"),
            ],
        )

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

        Returns:
            Search results persons matching the query.

        Examples:

           Search for 'my_person' in all text properties:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> persons = client.person.search('my_person')

        """
        filter_ = _create_person_filter(
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

        Returns:
            Aggregation results.

        Examples:

            Count persons in space `my_space`:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.person.aggregate("count", space="my_space")

        """

        filter_ = _create_person_filter(
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

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_person_filter(
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

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> persons = client.person.list(limit=5)

        """
        filter_ = _create_person_filter(
            self._view_id,
            min_birth_year,
            max_birth_year,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            space=space,
            retrieve_edges=retrieve_edges,
            edge_api_name_pairs=[
                (self.roles_edge, "roles"),
            ],
        )
