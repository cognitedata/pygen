from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core._api.cognite_file_query import CogniteFileQueryAPI
from cognite_core.data_classes import (
    CogniteAsset,
    CogniteEquipment,
    CogniteFile,
    CogniteFileCategory,
    CogniteFileFields,
    CogniteFileList,
    CogniteFileTextFields,
    CogniteFileWrite,
    CogniteFileWriteList,
    CogniteSourceSystem,
    ResourcesWriteResult,
)
from cognite_core.data_classes._cognite_file import (
    _COGNITEFILE_PROPERTIES_BY_FIELD,
    CogniteFileQuery,
    _create_cognite_file_filter,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
)


class CogniteFileAPI(NodeAPI[CogniteFile, CogniteFileWrite, CogniteFileList, CogniteFileWriteList]):
    _view_id = dm.ViewId("cdf_cdm", "CogniteFile", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITEFILE_PROPERTIES_BY_FIELD
    _class_type = CogniteFile
    _class_list = CogniteFileList
    _class_write_list = CogniteFileWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        category: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogniteFileQueryAPI[CogniteFileList]:
        """Query starting at Cognite files.

        Args:
            assets: The asset to filter on.
            category: The category to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            directory: The directory to filter on.
            directory_prefix: The prefix of the directory to filter on.
            is_uploaded: The is uploaded to filter on.
            mime_type: The mime type to filter on.
            mime_type_prefix: The prefix of the mime type to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            min_uploaded_time: The minimum value of the uploaded time to filter on.
            max_uploaded_time: The maximum value of the uploaded time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite files to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite files.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_file_filter(
            self._view_id,
            assets,
            category,
            description,
            description_prefix,
            directory,
            directory_prefix,
            is_uploaded,
            mime_type,
            mime_type_prefix,
            name,
            name_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            min_uploaded_time,
            max_uploaded_time,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(CogniteFileList)
        return CogniteFileQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        cognite_file: CogniteFileWrite | Sequence[CogniteFileWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite files.

        Args:
            cognite_file: Cognite file or
                sequence of Cognite files to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None.
                However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cognite_file:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CogniteFileWrite
                >>> client = CogniteCoreClient()
                >>> cognite_file = CogniteFileWrite(
                ...     external_id="my_cognite_file", ...
                ... )
                >>> result = client.cognite_file.apply(cognite_file)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_file.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_file, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite file.

        Args:
            external_id: External id of the Cognite file to delete.
            space: The space where all the Cognite file are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_file by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_file.delete("my_cognite_file")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_file.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CogniteFile | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CogniteFileList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteFile | CogniteFileList | None:
        """Retrieve one or more Cognite files by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite files.
            space: The space where all the Cognite files are located.

        Returns:
            The requested Cognite files.

        Examples:

            Retrieve cognite_file by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_file = client.cognite_file.retrieve(
                ...     "my_cognite_file"
                ... )

        """
        return self._retrieve(external_id, space, retrieve_edges=True, edge_api_name_type_direction_view_id_penta=[])

    def search(
        self,
        query: str,
        properties: CogniteFileTextFields | SequenceNotStr[CogniteFileTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        category: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteFileFields | SequenceNotStr[CogniteFileFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteFileList:
        """Search Cognite files

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            assets: The asset to filter on.
            category: The category to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            directory: The directory to filter on.
            directory_prefix: The prefix of the directory to filter on.
            is_uploaded: The is uploaded to filter on.
            mime_type: The mime type to filter on.
            mime_type_prefix: The prefix of the mime type to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            min_uploaded_time: The minimum value of the uploaded time to filter on.
            max_uploaded_time: The maximum value of the uploaded time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite files to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite files matching the query.

        Examples:

           Search for 'my_cognite_file' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_files = client.cognite_file.search(
                ...     'my_cognite_file'
                ... )

        """
        filter_ = _create_cognite_file_filter(
            self._view_id,
            assets,
            category,
            description,
            description_prefix,
            directory,
            directory_prefix,
            is_uploaded,
            mime_type,
            mime_type_prefix,
            name,
            name_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            min_uploaded_time,
            max_uploaded_time,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: CogniteFileFields | SequenceNotStr[CogniteFileFields] | None = None,
        query: str | None = None,
        search_property: CogniteFileTextFields | SequenceNotStr[CogniteFileTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        category: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: CogniteFileFields | SequenceNotStr[CogniteFileFields] | None = None,
        query: str | None = None,
        search_property: CogniteFileTextFields | SequenceNotStr[CogniteFileTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        category: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: CogniteFileFields | SequenceNotStr[CogniteFileFields],
        property: CogniteFileFields | SequenceNotStr[CogniteFileFields] | None = None,
        query: str | None = None,
        search_property: CogniteFileTextFields | SequenceNotStr[CogniteFileTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        category: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: CogniteFileFields | SequenceNotStr[CogniteFileFields] | None = None,
        property: CogniteFileFields | SequenceNotStr[CogniteFileFields] | None = None,
        query: str | None = None,
        search_property: CogniteFileTextFields | SequenceNotStr[CogniteFileTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        category: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite files

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            assets: The asset to filter on.
            category: The category to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            directory: The directory to filter on.
            directory_prefix: The prefix of the directory to filter on.
            is_uploaded: The is uploaded to filter on.
            mime_type: The mime type to filter on.
            mime_type_prefix: The prefix of the mime type to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            min_uploaded_time: The minimum value of the uploaded time to filter on.
            max_uploaded_time: The maximum value of the uploaded time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite files to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite files in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_file.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_file_filter(
            self._view_id,
            assets,
            category,
            description,
            description_prefix,
            directory,
            directory_prefix,
            is_uploaded,
            mime_type,
            mime_type_prefix,
            name,
            name_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            min_uploaded_time,
            max_uploaded_time,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: CogniteFileFields,
        interval: float,
        query: str | None = None,
        search_property: CogniteFileTextFields | SequenceNotStr[CogniteFileTextFields] | None = None,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        category: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite files

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            assets: The asset to filter on.
            category: The category to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            directory: The directory to filter on.
            directory_prefix: The prefix of the directory to filter on.
            is_uploaded: The is uploaded to filter on.
            mime_type: The mime type to filter on.
            mime_type_prefix: The prefix of the mime type to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            min_uploaded_time: The minimum value of the uploaded time to filter on.
            max_uploaded_time: The maximum value of the uploaded time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite files to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_file_filter(
            self._view_id,
            assets,
            category,
            description,
            description_prefix,
            directory,
            directory_prefix,
            is_uploaded,
            mime_type,
            mime_type_prefix,
            name,
            name_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            min_uploaded_time,
            max_uploaded_time,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def query(self) -> CogniteFileQuery:
        """Start a query for Cognite files."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return CogniteFileQuery(self._client)

    def select(self) -> CogniteFileQuery:
        """Start selecting from Cognite files."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return CogniteFileQuery(self._client)

    def list(
        self,
        assets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        category: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        directory: str | list[str] | None = None,
        directory_prefix: str | None = None,
        is_uploaded: bool | None = None,
        mime_type: str | list[str] | None = None,
        mime_type_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        source_context: str | list[str] | None = None,
        source_context_prefix: str | None = None,
        min_source_created_time: datetime.datetime | None = None,
        max_source_created_time: datetime.datetime | None = None,
        source_created_user: str | list[str] | None = None,
        source_created_user_prefix: str | None = None,
        source_id: str | list[str] | None = None,
        source_id_prefix: str | None = None,
        min_source_updated_time: datetime.datetime | None = None,
        max_source_updated_time: datetime.datetime | None = None,
        source_updated_user: str | list[str] | None = None,
        source_updated_user_prefix: str | None = None,
        min_uploaded_time: datetime.datetime | None = None,
        max_uploaded_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteFileFields | Sequence[CogniteFileFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CogniteFileList:
        """List/filter Cognite files

        Args:
            assets: The asset to filter on.
            category: The category to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            directory: The directory to filter on.
            directory_prefix: The prefix of the directory to filter on.
            is_uploaded: The is uploaded to filter on.
            mime_type: The mime type to filter on.
            mime_type_prefix: The prefix of the mime type to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source: The source to filter on.
            source_context: The source context to filter on.
            source_context_prefix: The prefix of the source context to filter on.
            min_source_created_time: The minimum value of the source created time to filter on.
            max_source_created_time: The maximum value of the source created time to filter on.
            source_created_user: The source created user to filter on.
            source_created_user_prefix: The prefix of the source created user to filter on.
            source_id: The source id to filter on.
            source_id_prefix: The prefix of the source id to filter on.
            min_source_updated_time: The minimum value of the source updated time to filter on.
            max_source_updated_time: The maximum value of the source updated time to filter on.
            source_updated_user: The source updated user to filter on.
            source_updated_user_prefix: The prefix of the source updated user to filter on.
            min_uploaded_time: The minimum value of the uploaded time to filter on.
            max_uploaded_time: The maximum value of the uploaded time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite files to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `assets`, `category`, `equipment` and `source` for the Cognite
            files. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the
            identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested Cognite files

        Examples:

            List Cognite files and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_files = client.cognite_file.list(limit=5)

        """
        filter_ = _create_cognite_file_filter(
            self._view_id,
            assets,
            category,
            description,
            description_prefix,
            directory,
            directory_prefix,
            is_uploaded,
            mime_type,
            mime_type_prefix,
            name,
            name_prefix,
            source,
            source_context,
            source_context_prefix,
            min_source_created_time,
            max_source_created_time,
            source_created_user,
            source_created_user_prefix,
            source_id,
            source_id_prefix,
            min_source_updated_time,
            max_source_updated_time,
            source_updated_user,
            source_updated_user_prefix,
            min_uploaded_time,
            max_uploaded_time,
            external_id_prefix,
            space,
            filter,
        )

        if retrieve_connections == "skip":
            return self._list(
                limit=limit,
                filter=filter_,
                sort_by=sort_by,  # type: ignore[arg-type]
                direction=direction,
                sort=sort,
            )

        builder = DataClassQueryBuilder(CogniteFileList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                CogniteFile,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteEquipment._view_id]),
                        direction="inwards",
                        through=CogniteEquipment._view_id.as_property_ref("files"),
                    ),
                    CogniteEquipment,
                    connection_type="reverse-list",
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteAsset._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("assets"),
                    ),
                    CogniteAsset,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteFileCategory._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("category"),
                    ),
                    CogniteFileCategory,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteSourceSystem._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("source"),
                    ),
                    CogniteSourceSystem,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
