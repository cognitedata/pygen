from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

EXAMPLES_DIR = REPO_ROOT / "examples"

CINEMATOGRAPHY = EXAMPLES_DIR / "cinematography_domain"
SCHEMAS_DIR = REPO_ROOT / "tests" / "schemas"


class TestSchemas:
    foobar = SCHEMAS_DIR / "foobar.graphql"
    case_scenario = SCHEMAS_DIR / "case_scenario.graphql"
