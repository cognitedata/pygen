from ._core import (
    DataRecord,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    ResourcesWriteResult,
)
from ._scenario_instance import (
    ScenarioInstance,
    ScenarioInstanceApply,
    ScenarioInstanceApplyList,
    ScenarioInstanceFields,
    ScenarioInstanceGraphQL,
    ScenarioInstanceList,
    ScenarioInstanceTextFields,
    ScenarioInstanceWrite,
    ScenarioInstanceWriteList,
)


_GRAPHQL_DATA_CLASS_BY_VIEW_ID = {_cls.view_id: _cls for _cls in GraphQLCore.__subclasses__()}

__all__ = [
    "DataRecord",
    "DataRecordWrite",
    "ResourcesWrite",
    "DomainModel",
    "DomainModelCore",
    "DomainModelWrite",
    "DomainModelList",
    "DomainRelationWrite",
    "ResourcesWriteResult",
    "ScenarioInstance",
    "ScenarioInstanceGraphQL",
    "ScenarioInstanceWrite",
    "ScenarioInstanceApply",
    "ScenarioInstanceList",
    "ScenarioInstanceWriteList",
    "ScenarioInstanceApplyList",
    "ScenarioInstanceFields",
    "ScenarioInstanceTextFields",
]
