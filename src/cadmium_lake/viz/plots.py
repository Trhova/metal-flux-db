from __future__ import annotations

import matplotlib.pyplot as plt
import polars as pl

from cadmium_lake.io import read_duckdb_table
from cadmium_lake.paths import PLOTS_DIR


def build_basic_plots() -> list[str]:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    outputs: list[str] = []
    normalized = read_duckdb_table("measurements_normalized")
    raw = read_duckdb_table("measurements_raw")
    samples = read_duckdb_table("samples")
    studies = read_duckdb_table("studies_or_batches")

    if not normalized.is_empty() and not raw.is_empty() and not samples.is_empty():
        frame = normalized.join(raw.select(["measurement_id", "sample_id"]), on="measurement_id").join(
            samples.select(["sample_id", "matrix_group"]), on="sample_id"
        )
        pdf = frame.to_pandas()
        if not pdf.empty:
            fig, ax = plt.subplots(figsize=(8, 4))
            for layer, subset in pdf.groupby("matrix_group"):
                vals = subset["canonical_value"].dropna()
                if len(vals) > 0:
                    ax.hist(vals, bins=20, alpha=0.5, label=layer)
            ax.set_xscale("log")
            ax.set_title("Cadmium distributions by layer")
            ax.set_xlabel("Canonical value (layer-specific unit)")
            ax.legend()
            path = PLOTS_DIR / "layer_distributions.png"
            fig.tight_layout()
            fig.savefig(path)
            plt.close(fig)
            outputs.append(str(path))

            fig, ax = plt.subplots(figsize=(8, 4))
            pdf["log10_value"] = pdf["canonical_value"].apply(lambda x: None if x is None or x <= 0 else __import__("math").log10(x))
            for layer, subset in pdf.groupby("matrix_group"):
                vals = subset["log10_value"].dropna()
                if len(vals) > 0:
                    ax.hist(vals, bins=20, alpha=0.5, label=layer)
            ax.set_title("Cross-layer normalized variability")
            ax.set_xlabel("log10(canonical_value)")
            ax.legend()
            path = PLOTS_DIR / "cross_layer_variability.png"
            fig.tight_layout()
            fig.savefig(path)
            plt.close(fig)
            outputs.append(str(path))

    if not samples.is_empty():
        coverage = samples.group_by("country").len().sort("len", descending=True).head(10)
        if not coverage.is_empty():
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.bar(coverage["country"].fill_null("unknown"), coverage["len"])
            ax.set_title("Source coverage by geography")
            ax.set_ylabel("Sample count")
            path = PLOTS_DIR / "coverage_by_geography.png"
            fig.tight_layout()
            fig.savefig(path)
            plt.close(fig)
            outputs.append(str(path))

    if not studies.is_empty():
        years = studies.select(["study_id", "year_start"]).drop_nulls().group_by("year_start").len().sort("year_start")
        if not years.is_empty():
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.plot(years["year_start"], years["len"], marker="o")
            ax.set_title("Source coverage by year")
            ax.set_ylabel("Study count")
            path = PLOTS_DIR / "coverage_by_year.png"
            fig.tight_layout()
            fig.savefig(path)
            plt.close(fig)
            outputs.append(str(path))

    return outputs
