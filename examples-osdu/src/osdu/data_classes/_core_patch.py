# TODO: 230925 pa: dependencies
from typing import Any, Callable, ClassVar, Optional

from pydantic import ConfigDict, Field
from pydantic.functional_validators import model_validator


class DomainModelApplyPatch:
    """Summarizing all changes needed for DomainModelApply to support
    Lazy `external_id` validation and setting.
    """

    external_id_factory: ClassVar[Callable[[Any], str]]  # = Field(repr=False)

    @classmethod
    def set_external_id_factory(cls, factory: Callable[[Any], str]):
        cls.external_id_factory = factory

    # TODO: pa 230925: `populate_by_name` ex v1 `allow_population_by_field_name`
    #   - https://docs.pydantic.dev/latest/usage/model_config/#extra-attributes
    #   - https://docs.pydantic.dev/latest/usage/model_config/
    #   - https://docs.pydantic.dev/latest/migration/#changes-to-config
    model_config = ConfigDict(
        # `pydantic.config.Extra` is deprecated, use literal values instead (e.g. `extra='allow'`)
        extra="forbid",
        populate_by_name=True,
    )
    external_id: Optional[str] = Field(None, min_length=1, max_length=255)

    @model_validator(mode="after")
    def post_check_extid(self):
        # set a value for ext-id (placeholder for testing)
        if not self.external_id:
            # call out to a func and config how to create the ext-id
            self.external_id = DomainModelApplyPatch.external_id_factory(self)
        else:
            domain_model_name = type(self).__name__[:-5]  # strip suffix 'Apply'
            print(f"{domain_model_name} extid already set `{self.external_id}`")
        return self
