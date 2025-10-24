"""Base classes and helpers for MacroBase classifiers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Sequence

import pandas as pd


class Classifier(ABC):
    """Abstract base classifier with fit/score/predict lifecycle."""

    def __init__(self) -> None:
        self._fitted: bool = False

    @property
    def fitted(self) -> bool:
        """Whether the classifier has been trained."""

        return self._fitted

    def _mark_fitted(self) -> None:
        self._fitted = True

    def _ensure_fitted(self) -> None:
        if not self._fitted:
            raise RuntimeError("Classifier must be fitted before use.")

    @abstractmethod
    def fit(self, data: pd.DataFrame) -> "Classifier":
        """Fit the classifier using the provided data."""

    @abstractmethod
    def score(self, data: pd.DataFrame) -> pd.Series:
        """Compute outlier scores for the provided data."""

    @abstractmethod
    def predict(self, data: pd.DataFrame) -> pd.Series:
        """Return boolean predictions indicating outliers."""


def ensure_columns(data: pd.DataFrame, columns: Sequence[str]) -> None:
    """Validate that the requested columns are present in *data*."""

    missing = [column for column in columns if column not in data.columns]
    if missing:
        raise KeyError(f"Missing required columns: {', '.join(missing)}")


def to_series(values: Iterable[float], index: pd.Index) -> pd.Series:
    """Utility to wrap iterables into a :class:`pandas.Series`."""

    return pd.Series(list(values), index=index)

