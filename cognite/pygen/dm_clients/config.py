import os
import sys

from dynaconf import Dynaconf

__all__ = ["settings"]


if "__main__" not in sys.modules:
    os.environ["CORE_LOADERS_FOR_DYNACONF"] = "[]"
    os.environ["SETTINGS_FILES_FOR_DYNACONF"] = ""
    # os.environ["LOADERS_FOR_DYNACONF"] = "['path.to.settings_loader']"


settings = Dynaconf(
    envvar_prefix="SETTINGS",
    ignore_unknown_envvars=True,
    settings_files=os.environ.get("SETTINGS_FILES_FOR_DYNACONF", "settings.toml;.secrets.toml"),
    merge_enabled=True,
)


# `envvar_prefix` = export envvars with `export DM_CLIENTS_FOO=bar`.
# `settings_files` = Load these files in the order.


# In Cognite Functions, we need to disable Dynaconf's file lookup. It uses Python's `inspect` module which
# relies on `sys.modules["__main__"]` value, but this value seems to be missing in Functions.
# We can still make use of settings by creating a module with a `load` and set a path to is (see below).

# def load(obj, env=None, silent=True, key=None, filename=None):
#     """
#     Populate settings object from Python.
#     For this to work:
#      1. make a new module (e.g. `settings_loader`),
#      2. copy this function there and edit values as needed (and uncomment),
#      3. set `LOADERS_FOR_DYNACONF` env to python dotted path of the new module
#         (without ".load", as a JSON array, see example above).
#     """
#     obj.update(
#         {
#             "DM_CLIENTS": {
#                 "space": "CogShop2",
#                 "datamodel": "CogShop2",
#                 "schema_version": 1,
#                 "max_tries": 5,
#             },
#             "LOCAL": {
#                 "name": "PowerOps",
#                 "graphql_schema": "common/cdf_dm/schema.graphql",
#                 "schema_module": "common/cdf_dm/schema.py",
#             },
#         }
#     )
