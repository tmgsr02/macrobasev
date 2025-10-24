"""Simple Streamlit demo for MacroBase Python analytics."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

try:
    import streamlit as st
except ImportError as exc:  # pragma: no cover - Streamlit optional
    raise SystemExit("Streamlit is required to run this demo. Install it via `pip install streamlit`.") from exc

from macrobase_py.api import analyze_dataframe


SUPPORTED_TYPES = {
    ".csv": "csv",
    ".tsv": "tsv",
    ".txt": "csv",
    ".xls": "excel",
    ".xlsx": "excel",
    ".parquet": "parquet",
}


def _load_uploaded_file(path: Path, data) -> pd.DataFrame:
    ext = path.suffix.lower()
    kind = SUPPORTED_TYPES.get(ext)
    if kind == "csv":
        sep = "\t" if ext == ".tsv" else ","
        return pd.read_csv(data, sep=sep)
    if kind == "excel":
        return pd.read_excel(data)
    if kind == "parquet":
        return pd.read_parquet(data)
    raise ValueError(f"Unsupported file type: {ext}")


def main() -> None:
    st.set_page_config(page_title="MacroBase Explorer", layout="wide")
    st.title("MacroBase Explorer")
    st.write("Upload a tabular dataset to compute quick summaries and anomaly indicators.")

    uploaded_file = st.file_uploader(
        "Upload CSV, TSV, Excel, or Parquet files", type=[ext.lstrip(".") for ext in SUPPORTED_TYPES]
    )

    if uploaded_file is None:
        st.info("Awaiting file upload…")
        return

    dataset_name = uploaded_file.name
    try:
        dataframe = _load_uploaded_file(Path(dataset_name), uploaded_file)
    except Exception as exc:  # pragma: no cover - UI feedback
        st.error(f"Unable to load file: {exc}")
        return

    st.success(f"Loaded `{dataset_name}` with {len(dataframe):,} rows and {dataframe.shape[1]} columns.")

    target_column: Optional[str] = None
    if not dataframe.select_dtypes(include=["number"]).columns.empty:
        target_column = st.selectbox(
            "Select a numeric column to highlight in the correlation matrix",
            options=["(none)"] + list(dataframe.select_dtypes(include=["number"]).columns),
        )
        if target_column == "(none)":
            target_column = None

    pivot_column = st.selectbox("Optional pivot index", options=["(none)"] + list(dataframe.columns))
    pivot_metric = st.selectbox(
        "Metric to aggregate", options=["(auto)"] + list(dataframe.select_dtypes(include=["number"]).columns)
    )

    config = {}
    if target_column is not None:
        config["target_column"] = target_column
    if pivot_column != "(none)" and pivot_metric != "(auto)":
        config["pivot"] = [
            {
                "index": pivot_column,
                "values": [pivot_metric],
                "aggfunc": "mean",
            }
        ]

    result = analyze_dataframe(dataframe, config)

    st.subheader("Column Profiles")
    st.dataframe(pd.DataFrame([profile.__dict__ for profile in result.column_profiles]))

    st.subheader("Summary Statistics")
    st.dataframe(result.summary_statistics)

    if result.correlation_matrix is not None:
        st.subheader("Correlation Matrix")
        st.dataframe(result.correlation_matrix)

    if result.pivot_views:
        st.subheader("Pivot Views")
        for pivot in result.pivot_views:
            for metric, table in pivot.metrics.items():
                st.write(f"**{metric}**")
                st.dataframe(table)

if __name__ == "__main__":  # pragma: no cover - interactive app
    main()
