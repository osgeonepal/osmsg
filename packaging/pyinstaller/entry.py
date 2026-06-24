"""Frozen-app entry point: PyInstaller needs a real script to launch the Typer CLI."""

from osmsg.cli import app

if __name__ == "__main__":
    app()
