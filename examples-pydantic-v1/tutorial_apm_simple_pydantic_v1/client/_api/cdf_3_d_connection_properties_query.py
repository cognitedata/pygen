from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from tutorial_apm_simple_pydantic_v1.client.data_classes import (
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
)
from tutorial_apm_simple_pydantic_v1.client.data_classes._cdf_3_d_connection_properties import (
    _CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD,
)


class CdfConnectionPropertiesQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_cdf_3_d_connection_property: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_cdf_3_d_connection_property: Whether to retrieve the cdf 3 d connection property or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_cdf_3_d_connection_property and not self._builder[-1].name.startswith(
            "cdf_3_d_connection_property"
        ):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("cdf_3_d_connection_property"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[CdfConnectionPropertiesApply],
                                list(_CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=CdfConnectionProperties,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
