"""Visualization helpers for MacroBase Python analyses."""
from __future__ import annotations

from typing import Any, Iterable, Optional

import matplotlib.pyplot as plt
import pandas as pd

try:  # Optional dependency
    import plotly.express as px
except Exception:  # pragma: no cover - Plotly is optional
    px = None  # type: ignore

from macrobase_py.api import AnalysisResult


def render_summary_table(result: AnalysisResult, ax: Optional[plt.Axes] = None, *, max_rows: int = 15) -> plt.Axes:
    """Render the summary statistics table using Matplotlib."""

    if ax is None:
        _, ax = plt.subplots(figsize=(10, min(max_rows, len(result.summary_statistics)) * 0.4 + 1))

    table_data = (
        result.summary_statistics.reset_index().rename(columns={"index": "column"}).head(max_rows).fillna("")
    )
    ax.axis("off")
    table = ax.table(cellText=table_data.values, colLabels=table_data.columns, loc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.4)
    ax.set_title("Summary Statistics", fontsize=14, pad=12)
    return ax


def plot_top_values(
    result: AnalysisResult,
    column: str,
    *,
    top_n: int = 10,
    ax: Optional[plt.Axes] = None,
    normalize: bool = True,
) -> plt.Axes:
    """Plot the most frequent values for a given column."""

    if ax is None:
        _, ax = plt.subplots(figsize=(8, 4))

    profile = next((p for p in result.column_profiles if p.name == column), None)
    if profile is None:
        raise ValueError(f"Column '{column}' is not present in the analysis result")

    if not profile.top_values:
        raise ValueError(f"Column '{column}' does not have categorical frequency information")

    value_counts = pd.Series({entry["value"]: entry["count"] for entry in profile.top_values})

    if normalize and value_counts.sum() != 0:
        value_counts = value_counts / value_counts.sum()

    value_counts.sort_values(ascending=False).head(top_n).plot(kind="bar", ax=ax)
    ax.set_ylabel("Proportion" if normalize else "Count")
    ax.set_title(f"Top {top_n} values for {column}")
    return ax


def plot_pivot_heatmap(result: AnalysisResult, metric: str, ax: Optional[plt.Axes] = None) -> plt.Axes:
    """Render a heatmap for the first pivot view containing ``metric``."""

    pivot = _find_pivot_metric(result, metric)
    if pivot is None:
        raise ValueError(f"No pivot view found with metric '{metric}'")

    data = _coerce_pivot_frame(pivot.metrics[metric], metric)

    if ax is None:
        _, ax = plt.subplots(figsize=(8, 6))

    im = ax.imshow(data.values, aspect="auto", cmap="viridis")
    ax.set_xticks(range(len(data.columns)))
    ax.set_xticklabels([str(col) for col in data.columns], rotation=45, ha="right")
    ax.set_yticks(range(len(data.index)))
    ax.set_yticklabels([str(idx) for idx in data.index])
    ax.set_title(f"Pivot heatmap ({metric})")
    plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    return ax


def plotly_pivot_table(result: AnalysisResult, metric: str, **kwargs: Any):  # pragma: no cover - Visualization only
    """Render a pivot table using Plotly (if available)."""

    if px is None:
        raise ImportError("plotly is not installed. Install plotly to enable interactive visualizations.")

    pivot = _find_pivot_metric(result, metric)
    if pivot is None:
        raise ValueError(f"No pivot view found with metric '{metric}'")

    data = _coerce_pivot_frame(pivot.metrics[metric], metric)
    fig = px.imshow(data.values, aspect="auto", color_continuous_scale="Viridis", **kwargs)
    fig.update_layout(
        title=f"Pivot heatmap ({metric})",
        xaxis_title=_format_axis_label(pivot.columns, "Columns"),
        yaxis_title=_format_axis_label(pivot.index, "Index"),
    )
    return fig


def _find_pivot_metric(result: AnalysisResult, metric: str):
    for pivot in result.pivot_views:
        if metric in pivot.metrics:
            return pivot
    return None


def _coerce_pivot_frame(data: pd.DataFrame | pd.Series, metric: str) -> pd.DataFrame:
    if isinstance(data, pd.Series):
        data = data.to_frame(name=metric)
    else:
        data = data.copy()

    if isinstance(data.index, pd.MultiIndex):
        data.index = [" | ".join(str(part) for part in idx) for idx in data.index]
    else:
        data.index = data.index.map(str)

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [" | ".join(str(part) for part in col) for col in data.columns]
    else:
        data.columns = data.columns.map(str)

    data.columns.name = None
    return data


def _format_axis_label(value: Optional[Iterable[str]] | str, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, (list, tuple)):
        return " | ".join(str(v) for v in value)
    return str(value)


__all__ = [
    "render_summary_table",
    "plot_top_values",
    "plot_pivot_heatmap",
    "plotly_pivot_table",
]
