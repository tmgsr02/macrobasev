"""Percentile-based outlier detection."""

from __future__ import annotations

from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from .base import Classifier, ensure_columns, to_series


class PercentileOutlierClassifier(Classifier):
    """Flag rows whose metrics exceed a configurable percentile."""

    def __init__(
        self,
        metrics: Sequence[str],
        percentile: float = 95.0,
        *,
        higher_is_outlier: bool = True,
    ) -> None:
        if not metrics:
            raise ValueError("At least one metric column must be provided.")
        if not (0.0 < percentile < 100.0):
            raise ValueError("Percentile must be between 0 and 100 (exclusive).")

        self.metrics = list(metrics)
        self.percentile = percentile / 100.0
        self.higher_is_outlier = higher_is_outlier
        self.thresholds: dict[str, float] = {}

    def fit(self, data: pd.DataFrame) -> "PercentileOutlierClassifier":
        ensure_columns(data, self.metrics)

        for metric in self.metrics:
            series = data[metric].dropna()
            if series.empty:
                raise ValueError(f"Metric '{metric}' does not contain any data.")

            quantile = self.percentile if self.higher_is_outlier else 1.0 - self.percentile
            self.thresholds[metric] = float(series.quantile(quantile))

        self._mark_fitted()
        return self

    def _metric_scores(self, data: pd.DataFrame) -> Iterable[float]:
        for _, row in data[self.metrics].iterrows():
            deltas = []
            for metric in self.metrics:
                value = row[metric]
                threshold = self.thresholds[metric]
                if self.higher_is_outlier:
                    deltas.append(value - threshold)
                else:
                    deltas.append(threshold - value)
            yield float(np.max(deltas))

    def score(self, data: pd.DataFrame) -> pd.Series:
        self._ensure_fitted()
        ensure_columns(data, self.metrics)

        return to_series(self._metric_scores(data), data.index)

    def predict(self, data: pd.DataFrame) -> pd.Series:
        scores = self.score(data)
        return scores > 0.0

