import hashlib
import json
from typing import Any


def from_camel_to_snake_case(value: str) -> str:
    return "".join(["_" + i.lower() if i.isupper() else i for i in value]).lstrip("_")


# TODO HACK: this is a minimal config representing a 3-column table
#  for the extid_factory
config_raw = """
# all missing entries will be handled by hash_dump
# pygen_domain_name|fdm_pk|extid_func
Well|id|
GeoContexts|geo_political_entity_id;geo_type_id|
Wellbore|id|
# example of multiple properties required for unique id
# UnitSet|value;unit|
"""

config = {
    line.strip().split("|")[0]: dict(zip(["pygen_domain_name", "fdm_pk", "extid_func"], line.split("|")))
    for line in config_raw.split("\n")
    if line.strip()  # skip empty lines
    if not line.startswith("#")  # skip commented lines
}


def hash_dump(node: Any) -> list[str]:
    """
    Got more complicated as expected, to create reliable hashes even
    with different order of elements in lists or dicts

    TODO: is this the smartes generic approach to create a hash?
    - covered: examples-osdu/tests/test_extid_factory.py

    - dump pydantic instance to dict and then
    - serialize the instance to a JSON string
    json_str = json.dumps(my_instance.dict(), sort_keys=True)

    - create a hash over the JSON string
    hash_obj = hashlib.sha256(json_str.encode())
    hash_str = hash_obj.hexdigest()
    """

    def sort_values(d):
        if isinstance(d, dict):
            return {k: sort_values(v) for k, v in sorted(d.items())}
        elif isinstance(d, list):
            # not enough, need to sort the list as well
            if d and isinstance(d[0], dict):
                # first elem is a dict
                # TODO: assuming all are dicts
                # which need keys=lambda for sorting

                try:
                    sorted_dict_list = sorted(
                        d,
                        key=lambda _a_dict: [
                            (_k, sort_values(_a_dict[_k] if _a_dict[_k] else "n/a")) for _k in sorted(_a_dict.keys())
                        ],
                    )
                except Exception as e:
                    print(d)
                    raise e
                sorted_dict_list_nested = [sort_values(v) for v in sorted_dict_list]
                return sorted_dict_list_nested
            else:
                # list of non-dicts
                return [sort_values(v) for v in sorted(d)]
        else:
            return d

    return [f"hash#{hashlib.sha256(json.dumps(sort_values(node.model_dump())).encode()).hexdigest()}"]


def extid_factory(node: Any, prefix="dw:osdu:") -> str:
    # register all helper funcs
    # each needs to return list[str]
    extid_helpers = {
        "hash_dump": hash_dump,
    }

    # TODO that's a HACK to get the domain-model!
    domain_model_name = type(node).__name__[:-5]  # strip suffix 'Apply'

    # TODO: bypass missing configs, and use hash_dump instead
    pk_definition = config.get(domain_model_name, {"fdm_pk": None, "extid_func": "hash_dump"})
    if pk_definition["fdm_pk"]:
        # create an external-id by concatenating a list of properties
        # and return as primary-key (pk)
        pks = [str(getattr(node, from_camel_to_snake_case(pk))) for pk in pk_definition["fdm_pk"].split(";")]
    elif pk_definition["extid_func"]:
        # lookup extid_func by name from extid_helpers
        # return a function that creates a primary-key (pk)
        # generic approach is `hash_dump()`
        pks = extid_helpers[pk_definition["extid_func"]](node)
    else:
        raise Exception(f"Cannot create extid for {node}")

    return f"{prefix}{domain_model_name}:{'|'.join(pks)}"
