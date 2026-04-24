from __future__ import annotations

import math
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from cadmium_lake.io import read_duckdb_table
from cadmium_lake.paths import PLOTS_DIR


LAYER_ORDER = ["water", "fertilizer", "soil", "crop", "food", "feces", "blood"]
SPARSE_LAYERS = ["fertilizer", "crop", "feces", "water"]
LAYER_COLORS = {
    "water": "#2878a6",
    "fertilizer": "#395c6b",
    "soil": "#8f5e3b",
    "crop": "#467d4b",
    "food": "#c4792a",
    "feces": "#6b4f3b",
    "blood": "#9f2f43",
}
INTERACTIVE_BASENAMES = {
    "atlas_layer_comparison",
    "atlas_matrix_distributions",
    "atlas_time_trends",
    "atlas_source_coverage",
    "atlas_coverage_by_country",
    "atlas_coverage_by_year",
    "atlas_water_distribution",
    "atlas_conceptual_sankey",
}
STATIC_BASENAMES = {
    "atlas_main_static",
    "atlas_sparse_layers_static",
    "atlas_time_trends_static",
    "atlas_coverage_static",
}


def build_basic_plots() -> list[str]:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    _remove_stale_plot_outputs()
    outputs: list[str] = []

    comparison = read_duckdb_table("layer_comparison_view")
    trends = read_duckdb_table("time_trend_view")
    coverage = read_duckdb_table("source_coverage_view")
    sankey = read_duckdb_table("sankey_layer_medians_view")

    if not comparison.is_empty():
        outputs.append(_plot_layer_comparison_interactive(comparison))
        outputs.append(_plot_matrix_distributions_interactive(comparison))
        outputs.append(_plot_country_coverage_interactive(comparison))
        water_html = _plot_water_distribution_interactive(comparison)
        if water_html:
            outputs.append(water_html)
        outputs.append(_plot_main_atlas_static(comparison))
        sparse_path = _plot_sparse_layers_static(comparison)
        if sparse_path:
            outputs.append(sparse_path)

    if not trends.is_empty():
        time_html = _plot_time_trends_interactive(trends)
        if time_html:
            outputs.append(time_html)
        year_html = _plot_year_coverage_interactive(trends)
        if year_html:
            outputs.append(year_html)
        time_pdf = _plot_time_trends_static(trends)
        if time_pdf:
            outputs.append(time_pdf)

    if not coverage.is_empty():
        outputs.append(_plot_source_coverage_interactive(coverage))
        outputs.append(_plot_coverage_static(coverage, comparison, trends))

    if not sankey.is_empty():
        sankey_html = _plot_conceptual_sankey(sankey)
        if sankey_html:
            outputs.append(sankey_html)

    return outputs


def _plot_layer_comparison_interactive(frame: pl.DataFrame) -> str:
    pdf = _comparison_pdf(frame)
    fig = px.strip(
        pdf,
        x="ppm_equivalent",
        y="layer",
        color="source_id",
        category_orders={"layer": ordered_layers(pdf["layer"].tolist())},
        log_x=True,
        hover_data={
            "canonical_value": True,
            "canonical_unit": True,
            "ppm_equivalent": ':.4g',
            "country": True,
            "year_for_plotting": True,
            "year_for_plotting_source": True,
            "source_id": True,
            "study_title": True,
            "citation": True,
            "doi": True,
            "raw_value_text": True,
            "raw_unit": True,
            "page_or_sheet": True,
            "table_or_figure": True,
        },
        title="Cadmium concentration atlas",
        labels={"ppm_equivalent": "ppm-equivalent", "layer": "matrix layer", "source_id": "source"},
    )
    fig.update_traces(jitter=0.32, marker={"size": 6, "opacity": 0.45})
    fig.update_layout(height=650, template="plotly_white")
    return _write_html(fig, "atlas_layer_comparison")


def _plot_matrix_distributions_interactive(frame: pl.DataFrame) -> str:
    pdf = _comparison_pdf(frame)
    fig = px.violin(
        pdf,
        x="layer",
        y="display_value",
        color="layer",
        category_orders={"layer": ordered_layers(pdf["layer"].tolist())},
        color_discrete_map=LAYER_COLORS,
        box=True,
        points=False,
        hover_data={
            "display_unit": True,
            "ppm_equivalent": ':.4g',
            "country": True,
            "year_for_plotting": True,
            "source_id": True,
            "study_title": True,
            "citation": True,
            "doi": True,
        },
        title="Cadmium distributions by matrix",
        labels={"display_value": "matrix-specific concentration", "layer": "matrix layer"},
    )
    fig.update_yaxes(type="log")
    fig.update_layout(height=650, template="plotly_white", showlegend=False)
    return _write_html(fig, "atlas_matrix_distributions")


def _plot_time_trends_interactive(frame: pl.DataFrame) -> str | None:
    pdf = _trend_pdf(frame)
    eligible = eligible_time_layers(pdf)
    if not eligible:
        return None
    pdf = pdf[pdf["layer"].isin(eligible)].copy()
    fig = px.scatter(
        pdf,
        x="year_for_plotting",
        y="display_value",
        color="source_id",
        facet_row="layer",
        facet_row_spacing=0.03,
        category_orders={"layer": eligible},
        hover_data={
            "canonical_unit": True,
            "ppm_equivalent": ':.4g',
            "country": True,
            "year_for_plotting_source": True,
            "source_id": True,
            "study_title": True,
            "citation": True,
            "doi": True,
            "raw_value_text": True,
        },
        title="Cadmium time trends",
        labels={"year_for_plotting": "year", "display_value": "cadmium level", "source_id": "source"},
    )
    fig.update_traces(marker={"size": 5, "opacity": 0.25, "color": "lightgray"})
    for idx, layer in enumerate(eligible, start=1):
        subset = pdf[pdf["layer"] == layer]
        yearly = subset.groupby("year_for_plotting", as_index=False)["display_value"].median()
        fig.add_trace(
            go.Scatter(
                x=yearly["year_for_plotting"],
                y=yearly["display_value"],
                mode="lines+markers",
                line={"color": LAYER_COLORS.get(layer, "#333333"), "width": 2.5},
                marker={"size": 6},
                showlegend=False,
                hovertemplate="%{x}: median %{y:.4g}<extra></extra>",
            ),
            row=idx,
            col=1,
        )
    fig.update_yaxes(type="log")
    fig.update_layout(height=max(500, 260 * len(eligible)), template="plotly_white")
    return _write_html(fig, "atlas_time_trends")


def _plot_source_coverage_interactive(frame: pl.DataFrame) -> str:
    pdf = frame.to_pandas().sort_values("sample_count", ascending=False)
    fig = make_subplots(
        rows=1,
        cols=3,
        subplot_titles=["Top sources by samples", "Top sources by measurements", "Top sources by summary rows"],
    )
    fig.add_trace(go.Bar(x=pdf["source_id"], y=pdf["sample_count"], marker_color="#395c6b"), row=1, col=1)
    fig.add_trace(go.Bar(x=pdf["source_id"], y=pdf["measurement_count"], marker_color="#467d4b"), row=1, col=2)
    fig.add_trace(go.Bar(x=pdf["source_id"], y=pdf["summary_measurement_count"], marker_color="#9f2f43"), row=1, col=3)
    fig.update_layout(height=500, template="plotly_white", title="Source coverage")
    return _write_html(fig, "atlas_source_coverage")


def _plot_water_distribution_interactive(frame: pl.DataFrame) -> str | None:
    pdf = _comparison_pdf(frame)
    pdf = pdf[pdf["layer"] == "water"].copy()
    if pdf.empty:
        return None
    fig = px.violin(
        pdf,
        x="matrix_subtype",
        y="display_value",
        color="matrix_subtype",
        box=True,
        points="all",
        hover_data={
            "display_unit": True,
            "ppm_equivalent": ':.4g',
            "country": True,
            "year_for_plotting": True,
            "source_id": True,
            "sample_name": True,
            "location_name": True,
            "raw_value_text": True,
            "raw_unit": True,
        },
        title="Cadmium in water by subtype",
        labels={"display_value": "cadmium in water (ug/L)", "matrix_subtype": "water subtype"},
    )
    fig.update_yaxes(type="log")
    fig.update_layout(height=600, template="plotly_white", showlegend=False)
    return _write_html(fig, "atlas_water_distribution")


def _plot_conceptual_sankey(frame: pl.DataFrame) -> str | None:
    pdf = frame.to_pandas()
    pdf = pdf[pdf["conceptual_flow_value"].notna() & (pdf["conceptual_flow_value"] > 0)].copy()
    if pdf.empty:
        return None
    labels = ordered_layers(list(pdf["source_layer"]) + list(pdf["target_layer"]))
    index = {label: idx for idx, label in enumerate(labels)}
    values = pdf["conceptual_flow_value"].tolist()
    fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                node={
                    "label": labels,
                    "color": [LAYER_COLORS.get(label, "#888888") for label in labels],
                    "pad": 18,
                    "thickness": 16,
                },
                link={
                    "source": [index[value] for value in pdf["source_layer"]],
                    "target": [index[value] for value in pdf["target_layer"]],
                    "value": values,
                    "customdata": pdf[
                        [
                            "source_median_ppm_equivalent",
                            "target_median_ppm_equivalent",
                            "source_measurement_count",
                            "target_measurement_count",
                        ]
                    ].to_numpy(),
                    "hovertemplate": (
                        "%{source.label} -> %{target.label}<br>"
                        "conceptual value: %{value:.4g} ppm-equivalent<br>"
                        "source median: %{customdata[0]:.4g}<br>"
                        "target median: %{customdata[1]:.4g}<br>"
                        "n source/target: %{customdata[2]} / %{customdata[3]}<extra></extra>"
                    ),
                },
            )
        ]
    )
    fig.update_layout(
        title="Conceptual cadmium pathway medians (not mass-balanced)",
        template="plotly_white",
        height=560,
    )
    return _write_html(fig, "atlas_conceptual_sankey")


def _plot_country_coverage_interactive(frame: pl.DataFrame) -> str:
    pdf = _comparison_pdf(frame)
    coverage = pdf["country"].fillna("unknown").value_counts().head(20).sort_values(ascending=True)
    fig = go.Figure(
        go.Bar(
            x=coverage.values,
            y=coverage.index,
            orientation="h",
            marker_color="#1f77b4",
            hovertemplate="%{y}: %{x} rows<extra></extra>",
        )
    )
    fig.update_layout(title="Top countries by row count", xaxis_title="row count", yaxis_title="country", template="plotly_white", height=650)
    return _write_html(fig, "atlas_coverage_by_country")


def _plot_year_coverage_interactive(frame: pl.DataFrame) -> str | None:
    pdf = _trend_pdf(frame)
    if pdf.empty:
        return None
    coverage = pdf.groupby(["year_for_plotting", "layer"], as_index=False).size()
    fig = px.line(
        coverage,
        x="year_for_plotting",
        y="size",
        color="layer",
        color_discrete_map=LAYER_COLORS,
        title="Sample coverage by year",
        labels={"year_for_plotting": "year", "size": "row count", "layer": "matrix layer"},
    )
    fig.update_layout(template="plotly_white", height=500)
    return _write_html(fig, "atlas_coverage_by_year")


def _plot_main_atlas_static(frame: pl.DataFrame) -> str:
    pdf = _comparison_pdf(frame)
    pdf = pdf[pdf["ppm_equivalent"] > 0].copy()
    layers = ordered_layers(pdf["layer"].tolist())
    fig, ax = plt.subplots(figsize=(11, 6.8))
    data = [pdf.loc[pdf["layer"] == layer, "log10_ppm_equivalent"].to_numpy() for layer in layers]
    positions = list(range(1, len(layers) + 1))
    vp = ax.violinplot(data, positions=positions, vert=False, showmeans=False, showmedians=False, showextrema=False)
    for body, layer in zip(vp["bodies"], layers):
        body.set_facecolor(LAYER_COLORS.get(layer, "#666666"))
        body.set_edgecolor("none")
        body.set_alpha(0.25)
    means = [values.mean() for values in data]
    ax.scatter(means, positions, marker="D", s=38, color="#111111", zorder=3, label="geometric mean")
    counts = pdf["layer"].value_counts()
    for pos, layer in zip(positions, layers):
        ax.text(
            1.01,
            pos,
            f"n>0={int(counts[layer])}",
            transform=ax.get_yaxis_transform(),
            va="center",
            ha="left",
            fontsize=9,
            color="#333333",
        )
    all_log_values = pdf["log10_ppm_equivalent"].dropna()
    lower = math.floor(all_log_values.min())
    upper = math.ceil(all_log_values.max())
    ticks = list(range(lower, upper + 1))
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{10 ** tick:g}" for tick in ticks])
    ax.set_xlabel("ppm-equivalent")
    ax.set_yticks(positions)
    ax.set_yticklabels(layers)
    ax.set_ylabel("")
    ax.set_title("Cadmium concentration atlas")
    ax.grid(axis="x", alpha=0.2)
    ax.legend(frameon=False, loc="lower right")
    fig.tight_layout()
    path = PLOTS_DIR / "atlas_main_static.pdf"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return str(path)


def _plot_sparse_layers_static(frame: pl.DataFrame) -> str | None:
    pdf = _comparison_pdf(frame)
    pdf = pdf[pdf["layer"].isin(SPARSE_LAYERS)].copy()
    layers = ordered_layers(pdf["layer"].tolist())
    if not layers:
        return None
    fig, axes = plt.subplots(len(layers), 1, figsize=(10, max(4.5, 2.8 * len(layers))), squeeze=False)
    for ax, layer in zip(axes.flatten(), layers):
        subset = pdf[pdf["layer"] == layer].sort_values("ppm_equivalent")
        y = [0.0] * len(subset)
        jitter = [(idx % 7 - 3) * 0.03 for idx in range(len(subset))]
        ax.scatter(subset["ppm_equivalent"], [yy + jj for yy, jj in zip(y, jitter)], s=30, color=LAYER_COLORS.get(layer, "#666666"), alpha=0.8)
        for _, row in subset.head(12).iterrows():
            label = row["source_id"]
            if isinstance(row["study_title"], str) and row["study_title"]:
                label = f"{label}: {row['study_title'][:35]}"
            ax.annotate(label, (row["ppm_equivalent"], 0), xytext=(4, 4), textcoords="offset points", fontsize=7, color="#333333")
        ax.set_xscale("log")
        ax.set_yticks([])
        ax.set_xlabel("ppm-equivalent")
        ax.set_title(f"{layer.capitalize()} detail")
        ax.grid(axis="x", alpha=0.2)
    fig.tight_layout()
    path = PLOTS_DIR / "atlas_sparse_layers_static.pdf"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return str(path)


def _plot_time_trends_static(frame: pl.DataFrame) -> str | None:
    pdf = _trend_pdf(frame)
    layers = eligible_time_layers(pdf)
    if not layers:
        return None
    fig, axes = plt.subplots(len(layers), 1, figsize=(10, max(4.5, 2.8 * len(layers))), squeeze=False)
    for ax, layer in zip(axes.flatten(), layers):
        subset = pdf[pdf["layer"] == layer].sort_values("year_for_plotting")
        ax.scatter(subset["year_for_plotting"], subset["display_value"], s=10, color="lightgray", alpha=0.5, rasterized=True)
        yearly = subset.groupby("year_for_plotting", as_index=False)["display_value"].median()
        ax.plot(yearly["year_for_plotting"], yearly["display_value"], color=LAYER_COLORS.get(layer, "#333333"), linewidth=2.0)
        ax.scatter(yearly["year_for_plotting"], yearly["display_value"], color=LAYER_COLORS.get(layer, "#333333"), s=18)
        ax.set_yscale("log")
        ax.set_title(f"{layer.capitalize()} time trend")
        ax.set_xlabel("year")
        ax.set_ylabel(subset["display_unit"].iloc[0] if not subset.empty else "level")
        ax.grid(alpha=0.2)
    fig.tight_layout()
    path = PLOTS_DIR / "atlas_time_trends_static.pdf"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return str(path)


def _plot_coverage_static(coverage_frame: pl.DataFrame, comparison_frame: pl.DataFrame, trend_frame: pl.DataFrame) -> str:
    cov = coverage_frame.to_pandas().sort_values("sample_count", ascending=False).head(12)
    comp = _comparison_pdf(comparison_frame)
    countries = comp["country"].fillna("unknown").value_counts().head(12).sort_values()
    trend_pdf = _trend_pdf(trend_frame)
    years = trend_pdf["year_for_plotting"].value_counts().sort_index() if not trend_pdf.empty else None

    fig, axes = plt.subplots(1, 3, figsize=(14, 5.5))
    axes[0].barh(cov["source_id"][::-1], cov["sample_count"][::-1], color="#395c6b")
    axes[0].set_title("Top sources")
    axes[0].set_xlabel("rows")
    axes[1].barh(countries.index, countries.values, color="#1f77b4")
    axes[1].set_title("Top countries")
    axes[1].set_xlabel("rows")
    if years is not None and len(years) > 0:
        axes[2].plot(years.index, years.values, color="#9f2f43", linewidth=2)
        axes[2].scatter(years.index, years.values, color="#9f2f43", s=18)
    axes[2].set_title("Coverage by year")
    axes[2].set_xlabel("year")
    axes[2].set_ylabel("rows")
    for ax in axes:
        ax.grid(axis="x", alpha=0.2)
    fig.tight_layout()
    path = PLOTS_DIR / "atlas_coverage_static.pdf"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return str(path)


def _comparison_pdf(frame: pl.DataFrame):
    pdf = frame.to_pandas()
    pdf = pdf[pdf["ppm_equivalent"] > 0].copy()
    pdf = pdf[pdf["layer"].isin(LAYER_ORDER)].copy()
    return pdf


def _trend_pdf(frame: pl.DataFrame):
    pdf = frame.to_pandas()
    pdf = pdf[(pdf["year_for_plotting"].notna()) & (pdf["display_value"] > 0)].copy()
    if not pdf.empty:
        pdf["year_for_plotting"] = pdf["year_for_plotting"].astype(int)
        pdf = pdf[pdf["layer"].isin(LAYER_ORDER)].copy()
    return pdf


def eligible_time_layers(pdf) -> list[str]:
    eligible: list[str] = []
    for layer in ordered_layers(pdf["layer"].tolist() if not pdf.empty else []):
        subset = pdf[pdf["layer"] == layer]
        if len(subset) >= 30 and subset["year_for_plotting"].nunique() >= 5:
            eligible.append(layer)
    return eligible


def ordered_layers(values: list[str]) -> list[str]:
    present = set(values)
    return [layer for layer in LAYER_ORDER if layer in present]


def _write_html(fig: go.Figure, basename: str) -> str:
    path = PLOTS_DIR / f"{basename}.html"
    fig.write_html(path, include_plotlyjs="cdn")
    return str(path)


def _remove_stale_plot_outputs() -> None:
    allowed = {f"{name}.html" for name in INTERACTIVE_BASENAMES} | {f"{name}.pdf" for name in STATIC_BASENAMES}
    for path in PLOTS_DIR.glob("*"):
        if path.is_file() and path.name not in allowed:
            path.unlink()
