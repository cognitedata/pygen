"""This module extracts code from the query builder module such that it can be
included in the jinja template."""

import ast
from functools import lru_cache
from pathlib import Path

THIS_FOLDER = Path(__file__).resolve().parent
QUERY_BUILDER_FILE = THIS_FOLDER / "query_builder.py"


@lru_cache(maxsize=1)
def _load_query_builder_file() -> str:
    return QUERY_BUILDER_FILE.read_text()


@lru_cache(maxsize=1)
def get_classes_code(class_names: frozenset[str]) -> str:
    source_file = _load_query_builder_file()
    source_lines = source_file.splitlines()
    tree = ast.parse(source_file)

    class_code: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef | ast.FunctionDef) and node.name in class_names:
            start_lineno = node.lineno - 1 - len(node.decorator_list)
            end_lineno = node.end_lineno
            class_code.append("\n".join(source_lines[start_lineno:end_lineno]))

    return "\n\n\n".join(class_code)


@lru_cache
def get_file_content(filename: str) -> str:
    return (THIS_FOLDER / filename).read_text(encoding="utf-8")


if __name__ == "__main__":
    print(
        get_classes_code(
            frozenset({"ViewPropertyId", "QueryReducingBatchSize", "QueryStep", "QueryBuilder", "QueryResultCleaner"})
        )
    )
