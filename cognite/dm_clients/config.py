from dynaconf import Dynaconf

__all__ = ["settings"]


settings = Dynaconf(
    envvar_prefix="DM_CLIENTS",
    ignore_unknown_envvars=True,
    settings_files=["settings.toml", ".secrets.toml"],
    merge_enabled=True,
)


# `envvar_prefix` = export envvars with `export DM_CLIENTS_FOO=bar`.
# `settings_files` = Load these files in the order.
