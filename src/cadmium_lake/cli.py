from __future__ import annotations

from enum import Enum

import typer

from cadmium_lake.normalize.pipeline import run_normalization
from cadmium_lake.paths import ensure_directories
from cadmium_lake.pipeline import fetch_sources, initialize_catalog_tables, parse_sources, run_literature_search
from cadmium_lake.qa.checks import run_qa_checks
from cadmium_lake.viz.plots import build_basic_plots
from cadmium_lake.viz.views import build_views


app = typer.Typer(help="Provenance-first cadmium data lake CLI")


class AnalysisPolicy(str, Enum):
    censored = "censored"
    half_lod = "half_lod"
    half_loq = "half_loq"
    zero = "zero"


@app.callback()
def main() -> None:
    ensure_directories()
    initialize_catalog_tables()


@app.command()
def fetch(source: str | None = typer.Option(default=None, help="Single source id to fetch")) -> None:
    results = fetch_sources(source=source)
    for source_id, count in results.items():
        if count >= 0:
            typer.echo(f"{source_id}: fetched {count} files")
        else:
            typer.echo(f"{source_id}: fetch failed")


@app.command()
def parse(source: str | None = typer.Option(default=None, help="Single source id to parse")) -> None:
    results = parse_sources(source=source)
    for source_id, count in results.items():
        if count >= 0:
            typer.echo(f"{source_id}: parsed {count} records")
        else:
            typer.echo(f"{source_id}: parse failed")


@app.command("literature-search")
def literature_search() -> None:
    results = run_literature_search()
    for name, count in results.items():
        typer.echo(f"{name}: {count}")


@app.command()
def normalize(
    analysis_policy: AnalysisPolicy = typer.Option(
        default=AnalysisPolicy.censored,
        help="Analysis-only nondetect handling policy for downstream use.",
    )
) -> None:
    frame = run_normalization()
    typer.echo(f"normalized rows: {frame.height} using policy={analysis_policy.value}")


@app.command()
def qa() -> None:
    outputs = run_qa_checks()
    for name, path in outputs.items():
        typer.echo(f"{name}: {path}")


@app.command("build-views")
def build_views_command(
    with_plots: bool = typer.Option(default=True, help="Build simple exploratory plots")
) -> None:
    build_views()
    typer.echo("views built")
    if with_plots:
        outputs = build_basic_plots()
        for output in outputs:
            typer.echo(f"plot: {output}")
