import re


def to_camel(string: str) -> str:
    """Convert snake_case_name to camelCaseName."""
    string_split = string.split("_")
    return string_split[0] + "".join(word.capitalize() for word in string_split[1:])


def to_pascal(string: str) -> str:
    """Convert snake_case_name to PascalCaseName."""
    camel = to_camel(string)
    return f"{camel[0].upper()}{camel[1:]}" if camel else ""


def to_snake(string: str) -> str:
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
    """
    words = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?=[A-Z][a-z]|\d|\W|$)|\d+", string)
    return "_".join(map(str.lower, words))
