from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from osdu_wells.client.data_classes import (
    Tags,
    TagsApply,
)
from osdu_wells.client.data_classes._tags import (
    _TAGS_PROPERTIES_BY_FIELD,
)


class TagsQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_tag: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_tag: Whether to retrieve the tag or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_tag and not self._builder[-1].name.startswith("tag"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("tag"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[TagsApply],
                                list(_TAGS_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Tags,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
