from __future__ import annotations

import math
import random

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
    summary = read_duckdb_table("summary_measurements")

    point_frame = _build_point_frame(normalized, raw, samples, summary)
    if not point_frame.is_empty():
        outputs.append(_plot_layer_points(point_frame))
        outputs.append(_plot_cross_layer_points(point_frame))

    if not samples.is_empty():
        coverage = samples.group_by("country").len().sort("len", descending=True).head(12)
        if not coverage.is_empty():
            fig, ax = plt.subplots(figsize=(9, 4.5))
            y = list(range(coverage.height))
            ax.scatter(coverage["len"], y, s=50, color="#1f77b4")
            ax.set_yticks(y)
            ax.set_yticklabels(coverage["country"].fill_null("unknown"))
            ax.set_xlabel("Sample count")
            ax.set_title("Source coverage by geography")
            ax.grid(axis="x", alpha=0.25)
            path = PLOTS_DIR / "coverage_by_geography.png"
            fig.tight_layout()
            fig.savefig(path)
            plt.close(fig)
            outputs.append(str(path))

    if not studies.is_empty():
        years = studies.select(["study_id", "year_start"]).drop_nulls().group_by("year_start").len().sort("year_start")
        if not years.is_empty():
            fig, ax = plt.subplots(figsize=(9, 4.5))
            ax.scatter(years["year_start"], years["len"], s=50, color="#d95f02")
            ax.plot(years["year_start"], years["len"], alpha=0.4, color="#d95f02")
            ax.set_title("Source coverage by year")
            ax.set_xlabel("Study year")
            ax.set_ylabel("Study count")
            ax.grid(alpha=0.25)
            path = PLOTS_DIR / "coverage_by_year.png"
            fig.tight_layout()
            fig.savefig(path)
            plt.close(fig)
            outputs.append(str(path))

    return outputs


def _build_point_frame(
    normalized: pl.DataFrame,
    raw: pl.DataFrame,
    samples: pl.DataFrame,
    summary: pl.DataFrame,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    if not normalized.is_empty() and not raw.is_empty() and not samples.is_empty():
        joined = (
            normalized.join(raw.select(["measurement_id", "sample_id"]), on="measurement_id", how="inner")
            .join(samples.select(["sample_id", "matrix_group"]), on="sample_id", how="left")
        )
        for record in joined.iter_rows(named=True):
            if record["canonical_value"] is None or record["canonical_unit"] is None or record["matrix_group"] is None:
                continue
            display = _to_display_dict(
                {"layer": record["matrix_group"], "value": record["canonical_value"], "unit": record["canonical_unit"]}
            )
            rows.append(
                {
                    "layer": record["matrix_group"],
                    "value": record["canonical_value"],
                    "unit": record["canonical_unit"],
                    "record_type": "individual",
                    **display,
                }
            )
    if not summary.is_empty():
        for record in summary.iter_rows(named=True):
            value = record["summary_value"]
            if value is None:
                value = record["upper_value"] if record["upper_value"] is not None else record["lower_value"]
            if value is None or record["summary_unit"] is None or record["matrix_group"] is None:
                continue
            display = _to_display_dict(
                {"layer": record["matrix_group"], "value": value, "unit": record["summary_unit"]}
            )
            rows.append(
                {
                    "layer": record["matrix_group"],
                    "value": value,
                    "unit": record["summary_unit"],
                    "record_type": "summary",
                    **display,
                }
            )
    if not rows:
        return pl.DataFrame()
    return pl.from_dicts(rows, infer_schema_length=None, strict=False)


def _to_display_dict(row: dict) -> dict[str, object]:
    layer = row["layer"]
    value = float(row["value"])
    unit = str(row["unit"])
    if layer in {"fertilizer", "soil", "plant", "food"} and unit == "mg/kg":
        return {"display_value": value, "display_unit": "ppm", "plot_label": f"{layer} (ppm)"}
    if layer == "gut" and unit == "fraction":
        return {"display_value": value * 100.0, "display_unit": "bioaccessible %", "plot_label": "gut (bioaccessible %)"}
    if unit == "ug/kg_bw/day":
        return {"display_value": value, "display_unit": "ug/kg bw/day", "plot_label": f"{layer} (ug/kg bw/day)"}
    return {"display_value": value, "display_unit": unit, "plot_label": f"{layer} ({unit})"}


def _plot_layer_points(frame: pl.DataFrame) -> str:
    pdf = frame.to_pandas()
    groups = list(pdf.groupby(["layer", "display_unit"], sort=True))
    cols = 2
    rows = math.ceil(len(groups) / cols) or 1
    fig, axes = plt.subplots(rows, cols, figsize=(12, max(4, rows * 3.4)))
    axes = axes.flatten() if hasattr(axes, "flatten") else [axes]
    rng = random.Random(7)
    colors = {"individual": "#1f77b4", "summary": "#d95f02"}
    for ax, ((layer, display_unit), subset) in zip(axes, groups):
        subset = subset[subset["display_value"] > 0]
        if subset.empty:
            ax.set_visible(False)
            continue
        jitter = [rng.uniform(-0.3, 0.3) for _ in range(len(subset))]
        for record_type, part in subset.groupby("record_type"):
            idx = part.index
            part_jitter = [jitter[list(subset.index).index(i)] for i in idx]
            ax.scatter(
                part["display_value"],
                part_jitter,
                s=10 if record_type == "individual" else 28,
                alpha=0.18 if record_type == "individual" else 0.9,
                color=colors[record_type],
                label=record_type if record_type not in ax.get_legend_handles_labels()[1] else None,
            )
        ax.set_xscale("log")
        ax.set_ylim(-0.45, 0.45)
        ax.set_yticks([])
        ax.set_xlabel(display_unit)
        ax.set_title(f"{layer.capitalize()} measurements")
        ax.grid(axis="x", alpha=0.2)
    for ax in axes[len(groups):]:
        ax.set_visible(False)
    handles, labels = axes[0].get_legend_handles_labels() if groups else ([], [])
    if handles:
        fig.legend(handles, labels, loc="upper right")
    fig.suptitle("Cadmium datapoints by layer in intelligible units", y=0.995)
    path = PLOTS_DIR / "layer_distributions.png"
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return str(path)


def _plot_cross_layer_points(frame: pl.DataFrame) -> str:
    pdf = frame.to_pandas()
    pdf = pdf[pdf["display_value"] > 0].copy()
    if pdf.empty:
        return str(PLOTS_DIR / "cross_layer_variability.png")
    pdf["layer_median"] = pdf.groupby("layer")["display_value"].transform("median")
    pdf["value_over_layer_median"] = pdf["display_value"] / pdf["layer_median"]
    pdf["layer_percentile"] = pdf.groupby("layer")["display_value"].rank(pct=True)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    rng = random.Random(11)
    order = list(dict.fromkeys(pdf["layer"]))
    for pos, layer in enumerate(order):
        subset = pdf[pdf["layer"] == layer]
        jitter = [pos + rng.uniform(-0.18, 0.18) for _ in range(len(subset))]
        axes[0].scatter(
            subset["value_over_layer_median"],
            jitter,
            s=10,
            alpha=0.18,
            color="#1f77b4",
        )
        axes[1].scatter(
            subset["layer_percentile"],
            jitter,
            s=10,
            alpha=0.18,
            color="#2ca02c",
        )
    axes[0].set_xscale("log")
    axes[0].set_yticks(range(len(order)))
    axes[0].set_yticklabels(order)
    axes[0].set_xlabel("value / layer median")
    axes[0].set_title("Cross-layer comparability")
    axes[0].grid(axis="x", alpha=0.2)
    axes[1].set_yticks(range(len(order)))
    axes[1].set_yticklabels(order)
    axes[1].set_xlabel("within-layer percentile")
    axes[1].set_title("Within-layer percentile spread")
    axes[1].grid(axis="x", alpha=0.2)
    path = PLOTS_DIR / "cross_layer_variability.png"
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return str(path)
