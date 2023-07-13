import re

import inflect


def to_camel(string: str, pluralize: bool = False, singularize: bool = False) -> str:
    """Convert snake_case_name to camelCaseName.
    >>> to_camel("a_b")
    'aB'
    >>> to_camel('camel_case', pluralize=True)
    'camelCases'
    >>> to_camel('best_directors', singularize=True)
    'bestDirector'
    """
    if "_" in string:
        # Is snake case
        string_split = string.split("_")
    else:
        # Assume is pascal/camel case
        # Ensure pascal
        string = string[0].upper() + string[1:]
        string_split = re.findall(r"[A-Z][a-z]*", string)
        if not string_split:
            string_split = [string]
    if pluralize and singularize:
        raise ValueError("Cannot pluralize and singularize at the same time")
    elif pluralize:
        string_split[-1] = as_plural(string_split[-1])
    elif singularize:
        string_split[-1] = as_singular(string_split[-1])
    try:
        return string_split[0] + "".join(word.capitalize() for word in string_split[1:])
    except IndexError:
        return ""


def to_pascal(string: str, pluralize=False, singularize: bool = False) -> str:
    """Convert snake_case_name to PascalCaseName.
    >>> to_pascal("a_b")
    'AB'
    >>> to_pascal('camel_case', pluralize=True)
    'CamelCases'
    >>> to_pascal('best_directors', singularize=True)
    'BestDirector'
    >>> to_pascal("BestLeadingActress", singularize=True)
    'BestLeadingActress'
    >>> to_pascal("priceScenarios", pluralize=True)
    'PriceScenarios'
    >>> to_pascal("reserveScenarios", pluralize=True)
    'ReserveScenarios'
    """
    camel = to_camel(string, pluralize, singularize)
    return f"{camel[0].upper()}{camel[1:]}" if camel else ""


def to_snake(string: str, pluralize: bool = False, singularize: bool = False) -> str:
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
    >>> to_snake('BestDirectors', singularize=True)
    'best_director'
    >>> to_snake('BestLeadingActress', pluralize=True)
    'best_leading_actresses'
    """
    words = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?=[A-Z][a-z]|\d|\W|$)|\d+", string)
    if pluralize and singularize:
        raise ValueError("Cannot pluralize and singularize at the same time")
    elif pluralize:
        words[-1] = as_plural(words[-1])
    elif singularize:
        words[-1] = as_singular(words[-1])
    return "_".join(map(str.lower, words))


# These are words which are not pluralized as we want to by inflect
# Fox example, "person" becomes "people" instead of "persons", which is more grammatically correct, however,
# "persons" is more consistent with the rest of the API.
_S_EXCEPTIONS = {"person", "persons"}

# These ase words that are incorrectly handled by inflect
_PLURAL_BY_SINGULAR = {"actress": "actresses"}
_SINGULAR_BY_PLURAL = {v: k for k, v in _PLURAL_BY_SINGULAR.items()}


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
    if noun.lower() in _SINGULAR_BY_PLURAL:
        return noun
    if noun.lower() in _PLURAL_BY_SINGULAR:
        return _PLURAL_BY_SINGULAR[noun.lower()]
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
    if noun.lower() in _PLURAL_BY_SINGULAR:
        return noun
    if noun.lower() in _SINGULAR_BY_PLURAL:
        return _SINGULAR_BY_PLURAL[noun.lower()]
    if (singular := _Inflect.engine().singular_noun(noun)) is False:
        return noun

    return singular
