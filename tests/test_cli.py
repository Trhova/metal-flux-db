from typer.testing import CliRunner

from cadmium_lake.cli import app


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "cadmium-lake" in result.stdout.lower() or "cadmium" in result.stdout.lower()
