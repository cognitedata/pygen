import subprocess
from pathlib import Path

import typer

app = typer.Typer()


def _run(bash_script):
    return subprocess.call(str(bash_script), shell=True)


@app.command()
def signin():
    """
    Sign in to a CDF project.
    Takes configuration from config.yaml.
    """
    _run(Path(__file__).parent / "bin/cdf_signin.sh")


schema_app = typer.Typer()


@schema_app.command()
def render():
    """
    Writes the GraphQL schema from Python code.
    Takes configuration from config.yaml.
    """
    _run(Path(__file__).parent / "bin/schema_render.sh")


@schema_app.command()
def publish():
    """
    Uploads the GraphQL schema file to DM.
    Takes configuration from config.yaml.
    """
    _run(Path(__file__).parent / "bin/schema_publish.sh")


app.add_typer(schema_app, name="schema")


def main():
    app()


if __name__ == "__main__":
    main()
