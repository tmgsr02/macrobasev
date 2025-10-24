"""Data loading utilities for MacroBase Python integration."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, Mapping, Optional

import pandas as pd
from pandas.api import types as pdt


@dataclass(frozen=True)
class ColumnMetadata:
    """Metadata describing a single column in a dataset."""

    name: str
    pandas_dtype: str
    logical_type: str
    nullable: bool
    null_count: int
    null_fraction: float
    unique_values: Optional[int] = None
    categorical: bool = False


class DataLoader:
    """Utility class for reading tabular data with schema inference.

    Parameters
    ----------
    source : str
        Path to the CSV or Excel file to load.
    file_type : Optional[str]
        Explicit file type (``"csv"`` or ``"excel"``). If not supplied the
        loader will infer the type from the file extension.
    chunk_size : Optional[int]
        Number of rows per chunk when iterating over data. Chunked loading is
        only supported for CSV inputs.
    read_options : Optional[Mapping[str, object]]
        Additional keyword arguments forwarded to ``pandas.read_csv`` or
        ``pandas.read_excel``.
    schema_overrides : Optional[Mapping[str, Mapping[str, object]]]
        Per-column overrides for the inferred schema. Keys may include
        ``logical_type``, ``pandas_dtype``, ``nullable`` and ``categorical``.
    sample_rows : int
        Number of rows to sample during schema inference.
    max_categorical_cardinality : int
        Upper bound on the number of unique values for an object column to be
        considered categorical.
    """

    SUPPORTED_FILE_TYPES = {"csv", "excel"}

    def __init__(
        self,
        source: str,
        file_type: Optional[str] = None,
        *,
        chunk_size: Optional[int] = None,
        read_options: Optional[Mapping[str, object]] = None,
        schema_overrides: Optional[Mapping[str, Mapping[str, object]]] = None,
        sample_rows: int = 500,
        max_categorical_cardinality: int = 50,
    ) -> None:
        self.source = Path(source)
        self.file_type = self._normalize_file_type(file_type)
        self.chunk_size = chunk_size
        self._base_read_options = dict(read_options or {})
        self.schema_overrides: Mapping[str, Mapping[str, object]] = (
            schema_overrides or {}
        )
        self.sample_rows = sample_rows
        self.max_categorical_cardinality = max_categorical_cardinality

        if self.chunk_size is not None and self.file_type != "csv":
            raise ValueError("Chunked loading is only supported for CSV files")

        self._schema: Dict[str, ColumnMetadata] = {}
        self._infer_schema()

    @property
    def schema(self) -> Mapping[str, ColumnMetadata]:
        """Return the inferred schema metadata."""

        return dict(self._schema)

    # ------------------------------------------------------------------
    # Public API
    def load(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load data into a typed :class:`pandas.DataFrame`.

        Parameters
        ----------
        limit : Optional[int]
            Maximum number of rows to read from the source. ``None`` reads the
            entire dataset.
        """

        df = self._read_full(limit=limit)
        return self._apply_schema(df)

    def iter_batches(self, chunk_size: Optional[int] = None) -> Iterator[pd.DataFrame]:
        """Yield dataframes in chunks using the inferred schema."""

        actual_chunk_size = chunk_size or self.chunk_size
        if actual_chunk_size is None:
            yield self.load()
            return

        for chunk in self._read_in_chunks(actual_chunk_size):
            yield self._apply_schema(chunk)

    # ------------------------------------------------------------------
    # Schema inference
    def _infer_schema(self) -> None:
        sample = self._load_sample()
        if sample is None or sample.empty:
            self._schema = {}
            return

        inferred: Dict[str, ColumnMetadata] = {}
        for column in sample.columns:
            series = sample[column]
            inferred[column] = self._infer_column(series)

        self._schema = inferred

    def _infer_column(self, series: pd.Series) -> ColumnMetadata:
        logical_type = self._detect_logical_type(series)
        pandas_dtype = str(series.dtype)
        null_count = int(series.isna().sum())
        total = len(series)
        null_fraction = float(null_count / total) if total > 0 else 0.0
        unique_values: Optional[int] = None
        categorical = logical_type == "categorical"

        try:
            unique_values = int(series.nunique(dropna=True))
        except TypeError:
            unique_values = None

        overrides = self.schema_overrides.get(series.name, {})
        logical_type = overrides.get("logical_type", logical_type)
        if logical_type == "boolean" and "pandas_dtype" not in overrides:
            pandas_dtype = "boolean"
        pandas_dtype = overrides.get("pandas_dtype", pandas_dtype)
        categorical = overrides.get("categorical", categorical)
        nullable = overrides.get("nullable", null_count > 0)

        return ColumnMetadata(
            name=series.name,
            pandas_dtype=pandas_dtype,
            logical_type=logical_type,
            nullable=nullable,
            null_count=null_count,
            null_fraction=null_fraction,
            unique_values=unique_values,
            categorical=categorical,
        )

    def _detect_logical_type(self, series: pd.Series) -> str:
        if pdt.is_bool_dtype(series):
            return "boolean"
        if pdt.is_datetime64_any_dtype(series):
            return "datetime"
        if pdt.is_numeric_dtype(series):
            return "numeric"
        if isinstance(series.dtype, pdt.CategoricalDtype):
            return "categorical"
        if pdt.is_object_dtype(series):
            non_null = series.dropna()
            if not non_null.empty:
                normalized = non_null.map(self._normalize_boolean_like)
                if normalized.notna().all():
                    return "boolean"

                datetime_coerced = pd.to_datetime(non_null, errors="coerce")
                valid_datetime_ratio = float(
                    datetime_coerced.notna().sum() / len(non_null)
                )
                if valid_datetime_ratio > 0.9:
                    return "datetime"

                numeric_coerced = pd.to_numeric(non_null, errors="coerce")
                valid_numeric_ratio = float(
                    numeric_coerced.notna().sum() / len(non_null)
                )
                if valid_numeric_ratio > 0.9:
                    return "numeric"
            unique_count = series.nunique(dropna=True)
            if unique_count <= self.max_categorical_cardinality:
                return "categorical"
            return "text"
        return "text"

    # ------------------------------------------------------------------
    # IO utilities
    def _normalize_file_type(self, file_type: Optional[str]) -> str:
        if file_type is not None:
            normalized = file_type.lower()
            if normalized not in self.SUPPORTED_FILE_TYPES:
                raise ValueError(f"Unsupported file type: {file_type}")
            return normalized

        suffix = self.source.suffix.lower()
        if suffix == ".csv":
            return "csv"
        if suffix in {".xls", ".xlsx"}:
            return "excel"
        raise ValueError(
            "Could not infer file type from extension; please specify file_type"
        )

    def _load_sample(self) -> Optional[pd.DataFrame]:
        options = self._reader_options()
        options.pop("chunksize", None)
        options.pop("iterator", None)
        if self.file_type == "csv":
            return pd.read_csv(self.source, nrows=self.sample_rows, **options)
        return pd.read_excel(self.source, nrows=self.sample_rows, **options)

    def _reader_options(self) -> Dict[str, object]:
        return dict(self._base_read_options)

    def _read_full(self, limit: Optional[int]) -> pd.DataFrame:
        options = self._reader_options()
        if self.file_type == "csv":
            if limit is not None:
                options["nrows"] = limit
            return pd.read_csv(self.source, **options)

        if limit is not None:
            options["nrows"] = limit
        return pd.read_excel(self.source, **options)

    def _read_in_chunks(self, chunk_size: int) -> Iterable[pd.DataFrame]:
        options = self._reader_options()
        options["chunksize"] = chunk_size
        if self.file_type == "csv":
            reader = pd.read_csv(self.source, **options)
            return reader
        raise ValueError("Chunked iteration is only implemented for CSV files")

    def _apply_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self._schema:
            return df

        typed_df = df.copy()
        for column, metadata in self._schema.items():
            if column not in typed_df.columns:
                continue

            series = typed_df[column]
            desired_logical = metadata.logical_type
            desired_dtype = metadata.pandas_dtype

            override = self.schema_overrides.get(column, {})
            if "pandas_dtype" in override:
                desired_dtype = override["pandas_dtype"]

            try:
                if desired_logical == "numeric":
                    typed_df[column] = pd.to_numeric(series, errors="coerce")
                elif desired_logical == "datetime":
                    typed_df[column] = pd.to_datetime(series, errors="coerce")
                elif desired_logical == "boolean":
                    typed_df[column] = self._coerce_boolean(series)
                elif metadata.categorical:
                    typed_df[column] = series.astype("category")
                elif desired_dtype not in (str(series.dtype), "object"):
                    typed_df[column] = series.astype(desired_dtype)
            except (ValueError, TypeError) as exc:
                raise ValueError(
                    f"Failed to cast column '{column}' to {desired_logical}/{desired_dtype}"
                ) from exc

        return typed_df

    def _normalize_boolean_like(self, value) -> Optional[bool]:
        if pd.isna(value):
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not pd.isna(value):
            if value == 1:
                return True
            if value == 0:
                return False
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "t", "yes", "y", "1"}:
                return True
            if normalized in {"false", "f", "no", "n", "0"}:
                return False
        return None

    def _coerce_boolean(self, series: pd.Series) -> pd.Series:
        coerced = [self._normalize_boolean_like(value) for value in series]
        return pd.Series(coerced, index=series.index, dtype="boolean")
