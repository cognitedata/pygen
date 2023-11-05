import builtins

PYTHON_BUILTIN_NAMES = {name for name in vars(builtins) if not name.startswith("_")}
FIELD_NAMES = {
    "space",
    "external_id",
    "version",
    "last_updated_time",
    "created_time",
    "deleted_time",
    "existing_version",
    "external_id_factory",
    "replace",
}
PARAMETER_NAMES = {
    "interval",
    "limit",
    "external_id",
    "space",
    "filter",
    "replace",
    "retrieve_edges",
    "property",
}

DATA_CLASS_NAMES = {"DomainModel", "DomainModelApply", "DomainModelList", "DomainModelApplyList"}
FILE_NAMES = {
    "__init__",
    "_core",
}
