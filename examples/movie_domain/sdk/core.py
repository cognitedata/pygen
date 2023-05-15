from pydantic import BaseModel, constr


class DomainModel(BaseModel):
    external_id: constr(min_length=1, max_length=255)


class TimeSeries(DomainModel):
    name: str


class TypeList:
    ...


class TypeAPI:
    def list(self, limit: int) -> TypeList:
        ...

    def apply(
        self,
    ):
        ...

    def retrieve(self) -> TypeList:
        ...

    def delete(self):
        ...
