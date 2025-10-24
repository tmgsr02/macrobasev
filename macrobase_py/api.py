"""High-level Python interface for the MacroBase analytical workflow.

This module exposes convenience wrappers for loading structured data sources,
computing summary statistics, identifying potentially interesting signals, and
returning results in a structured format that can be consumed by notebooks,
applications, and visualization utilities.
"""
from __future__ import annotations

import pathlib
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, MutableMapping, Optional

import numpy as np
import pandas as pd


@dataclass
class ColumnProfile:
    """Summary information about a single column."""

    name: str
    dtype: str
    non_nulls: int
    nulls: int
    unique: int
    sample_values: List[Any] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)
    top_values: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class PivotView:
    """Represents a pivoted aggregation over the dataset."""

    index: List[str]
    columns: Optional[List[str]]
    metrics: Dict[str, pd.DataFrame]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "columns": self.columns,
            "metrics": {name: frame.reset_index().to_dict(orient="records") for name, frame in self.metrics.items()},
        }


@dataclass
class AnalysisResult:
    """Container returned by :func:`analyze` with rich summary information."""

    row_count: int
    column_count: int
    column_profiles: List[ColumnProfile]
    summary_statistics: pd.DataFrame
    correlation_matrix: Optional[pd.DataFrame]
    pivot_views: List[PivotView] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "row_count": self.row_count,
            "column_count": self.column_count,
            "column_profiles": [profile.__dict__ for profile in self.column_profiles],
            "summary_statistics": self.summary_statistics.reset_index().rename(columns={"index": "metric"}).to_dict(orient="records"),
            "correlation_matrix": None
            if self.correlation_matrix is None
            else self.correlation_matrix.reset_index().rename(columns={"index": "feature"}).to_dict(orient="records"),
            "pivot_views": [pivot.to_dict() for pivot in self.pivot_views],
        }


def analyze(file_path: str | pathlib.Path, config: Optional[MutableMapping[str, Any]] = None) -> AnalysisResult:
    """Load a dataset and compute a bundle of summary statistics.

    Parameters
    ----------
    file_path:
        Path to the input dataset. CSV, TSV, Excel, and Parquet files are
        supported out of the box.
    config:
        Optional configuration dictionary. Recognized keys include:

        ``read_kwargs`` (``dict``)
            Additional keyword arguments passed to the pandas reader.
        ``target_column`` (``str``)
            Column name to treat as a target for correlation analysis.
        ``pivot`` (``list[dict]``)
            A collection of pivot specifications, each containing ``index``,
            optional ``columns``, ``values`` (list of metrics), and ``aggfunc``.

    Returns
    -------
    AnalysisResult
        Structured analysis output containing summary statistics, column level
        profiles, and optional pivot views.
    """

    if config is None:
        config = {}

    read_kwargs = dict(config.get("read_kwargs", {}))
    frame = _load_dataframe(file_path, read_kwargs)
    remaining_config = {key: value for key, value in config.items() if key != "read_kwargs"}
    return analyze_dataframe(frame, remaining_config)


def analyze_dataframe(frame: pd.DataFrame, config: Optional[MutableMapping[str, Any]] = None) -> AnalysisResult:
    if config is None:
        config = {}

    column_profiles = _profile_columns(frame)
    summary_stats = frame.describe(include="all").transpose()

    correlation_matrix: Optional[pd.DataFrame] = None
    target_column = config.get("target_column")
    numeric_frame = frame.select_dtypes(include=[np.number])
    if not numeric_frame.empty:
        correlation_matrix = numeric_frame.corr()
        if target_column is not None and target_column in correlation_matrix.columns:
            ordered_columns = [target_column] + [col for col in correlation_matrix.columns if col != target_column]
            correlation_matrix = correlation_matrix.loc[ordered_columns, ordered_columns]

    pivot_views: List[PivotView] = []
    for pivot_spec in config.get("pivot", []) or []:
        pivot_views.append(_build_pivot(frame, pivot_spec))

    return AnalysisResult(
        row_count=len(frame),
        column_count=frame.shape[1],
        column_profiles=column_profiles,
        summary_statistics=summary_stats,
        correlation_matrix=correlation_matrix,
        pivot_views=pivot_views,
    )


def _load_dataframe(file_path: str | pathlib.Path, read_kwargs: MutableMapping[str, Any]) -> pd.DataFrame:
    path = pathlib.Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {file_path}")

    ext = path.suffix.lower()
    if ext in {".csv", ".tsv", ".txt"}:
        sep = read_kwargs.pop("sep", "\t" if ext == ".tsv" else ",")
        return pd.read_csv(path, sep=sep, **read_kwargs)
    if ext in {".xls", ".xlsx"}:
        return pd.read_excel(path, **read_kwargs)
    if ext in {".parquet", ".pq"}:
        return pd.read_parquet(path, **read_kwargs)

    # Fall back to pandas' inference
    return pd.read_csv(path, **read_kwargs)


def _profile_columns(frame: pd.DataFrame) -> List[ColumnProfile]:
    profiles: List[ColumnProfile] = []
    head = frame.head(5)
    for column in frame.columns:
        series = frame[column]
        stats: Dict[str, Any] = {}
        if pd.api.types.is_numeric_dtype(series):
            stats = {
                "mean": series.mean(),
                "std": series.std(),
                "min": series.min(),
                "max": series.max(),
            }
        elif pd.api.types.is_datetime64_any_dtype(series):
            stats = {
                "min": series.min(),
                "max": series.max(),
            }

        counts = series.value_counts(dropna=True).head(5)
        top_values = [
            {
                "value": _format_value(idx),
                "count": int(count),
                "proportion": float(count / series.count()) if series.count() else 0.0,
            }
            for idx, count in counts.items()
        ]

        profile = ColumnProfile(
            name=column,
            dtype=str(series.dtype),
            non_nulls=series.count(),
            nulls=series.isna().sum(),
            unique=series.nunique(dropna=True),
            sample_values=head[column].dropna().tolist(),
            stats=stats,
            top_values=top_values,
        )
        profiles.append(profile)
    return profiles


def _build_pivot(frame: pd.DataFrame, spec: MutableMapping[str, Any]) -> PivotView:
    index = spec.get("index")
    if not index:
        raise ValueError("Pivot specification requires an 'index' key")

    columns = spec.get("columns")
    values = spec.get("values")
    aggfunc = spec.get("aggfunc", "mean")
    if values is None:
        numeric_columns = frame.select_dtypes(include=[np.number]).columns
        values = list(numeric_columns)

    pivoted = pd.pivot_table(frame, index=index, columns=columns, values=values, aggfunc=aggfunc)

    metrics: Dict[str, pd.DataFrame] = {}
    if isinstance(values, Iterable) and not isinstance(values, str):
        for value in values:
            subframe = pivoted[value] if value in pivoted else pivoted
            if isinstance(subframe, pd.Series):
                subframe = subframe.to_frame(name=str(value))
            metrics[str(value)] = subframe
    else:
        if isinstance(pivoted, pd.Series):
            pivoted = pivoted.to_frame(name=str(values))
        metrics[str(values)] = pivoted

    if isinstance(index, (list, tuple)):
        index_labels = [str(label) for label in index]
    else:
        index_labels = [str(index)]

    if columns is None:
        column_labels = None
    elif isinstance(columns, (list, tuple)):
        column_labels = [str(label) for label in columns]
    else:
        column_labels = [str(columns)]

    return PivotView(index=index_labels, columns=column_labels, metrics=metrics)


def _format_value(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return value.isoformat()
    if isinstance(value, float):
        return round(float(value), 6)
    return value


__all__ = ["analyze", "analyze_dataframe", "AnalysisResult", "ColumnProfile", "PivotView"]
