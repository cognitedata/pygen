import click
import typer

app = typer.Typer()


@app.command()
def hello(name: str = typer.Argument(..., help="Your name so I can great you.")):
    click.echo(f"Hello {name}")


@app.command()
def goodbye(name: str = ""):
    click.echo(f"Bye{f' {name}' if name else ''}!")


def main():
    app()
