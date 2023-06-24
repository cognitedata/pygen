import re

import inflect


def to_camel(string: str, pluralize: bool = False) -> str:
    """Convert snake_case_name to camelCaseName.
    >>> to_camel("a_b")
    'aB'
    >>> to_camel('camel_case', pluralize=True)
    'camelCases'
    """
    string_split = string.split("_")
    if pluralize:
        string_split[-1] = as_plural(string_split[-1])
    return string_split[0] + "".join(word.capitalize() for word in string_split[1:])


def to_pascal(string: str, pluralize=False) -> str:
    """Convert snake_case_name to PascalCaseName.
    >>> to_pascal("a_b")
    'AB'
    >>> to_pascal('camel_case', pluralize=True)
    'CamelCases'
    """
    camel = to_camel(string, pluralize)
    return f"{camel[0].upper()}{camel[1:]}" if camel else ""


def to_snake(string: str, pluralize: bool = False) -> str:
    """
    Convert PascalCaseName to snake_case_name.
    >>> to_snake("aB")
    'a_b'
    >>> to_snake('CamelCase')
    'camel_case'
    >>> to_snake('camelCamelCase')
    'camel_camel_case'
    >>> to_snake('Camel2Camel2Case')
    'camel_2_camel_2_case'
    >>> to_snake('getHTTPResponseCode')
    'get_http_response_code'
    >>> to_snake('get200HTTPResponseCode')
    'get_200_http_response_code'
    >>> to_snake('getHTTP200ResponseCode')
    'get_http_200_response_code'
    >>> to_snake('HTTPResponseCode')
    'http_response_code'
    >>> to_snake('ResponseHTTP')
    'response_http'
    >>> to_snake('ResponseHTTP2')
    'response_http_2'
    >>> to_snake('Fun?!awesome')
    'fun_awesome'
    >>> to_snake('Fun?!Awesome')
    'fun_awesome'
    >>> to_snake('10CoolDudes')
    '10_cool_dudes'
    >>> to_snake('20coolDudes')
    '20_cool_dudes'
    >>> to_snake('BestDirector', pluralize=True)
    'best_directors'
    """
    words = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?=[A-Z][a-z]|\d|\W|$)|\d+", string)
    if pluralize:
        words[-1] = as_plural(words[-1])
    return "_".join(map(str.lower, words))


# These are words which are not pluralized as we want to by inflect
# Fox example, "person" becomes "people" instead of "persons", which is more grammatically correct, however,
# "persons" is more consistent with the rest of the API.
_S_EXCEPTIONS = {"person", "persons"}


class _Inflect:
    _engine = None

    @classmethod
    def engine(cls) -> inflect.engine:
        if cls._engine is None:
            cls._engine = inflect.engine()
        return cls._engine


def as_plural(noun: str) -> str:
    """Pluralize a noun.
    >>> as_plural('person')
    'persons'
    >>> as_plural('Roles')
    'Roles'
    """
    if noun.lower() in _S_EXCEPTIONS:
        return f"{noun}s" if noun[-1] != "s" else noun
    if _Inflect.engine().singular_noun(noun) is False:
        return _Inflect.engine().plural_noun(noun)
    return noun


def as_singular(noun: str) -> str:
    """Singularize a noun.
    >>> as_singular('persons')
    'person'
    >>> as_singular('Roles')
    'Role'
    >>> as_singular('role')
    'role'
    """
    if noun.lower() in _S_EXCEPTIONS:
        return noun[:-1] if noun[-1] == "s" else noun
    if (singular := _Inflect.engine().singular_noun(noun)) is False:
        return noun

    return singular
