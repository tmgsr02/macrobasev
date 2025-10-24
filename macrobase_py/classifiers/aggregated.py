"""Classifiers designed for aggregated (cube) datasets."""

from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd

from .base import Classifier, ensure_columns


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, quantile: float) -> float:
    total = weights.sum()
    if total <= 0:
        raise ValueError("Weights must sum to a positive value.")
    order = np.argsort(values)
    values = values[order]
    weights = weights[order]
    cumulative = np.cumsum(weights)
    cutoff = quantile * total
    index = np.searchsorted(cumulative, cutoff, side="right")
    index = np.clip(index, 0, len(values) - 1)
    return float(values[index])


class AggregatedQuantileClassifier(Classifier):
    """Quantile based detector tailored to aggregated data."""

    def __init__(
        self,
        metrics: Sequence[str],
        *,
        quantile: float = 0.95,
        higher_is_outlier: bool = True,
        count_column: str | None = "count",
    ) -> None:
        if not metrics:
            raise ValueError("At least one metric column must be provided.")
        if not (0.0 < quantile < 1.0):
            raise ValueError("Quantile must be between 0 and 1 (exclusive).")
        self.metrics = list(metrics)
        self.quantile = float(quantile)
        self.higher_is_outlier = higher_is_outlier
        self.count_column = count_column
        self.thresholds: dict[str, float] = {}

    def fit(self, data: pd.DataFrame) -> "AggregatedQuantileClassifier":
        ensure_columns(data, self.metrics)
        weights = None
        if self.count_column is not None and self.count_column in data.columns:
            counts = data[self.count_column].to_numpy(dtype=float)
            if np.any(counts < 0):
                raise ValueError("Counts must be non-negative.")
            weights = counts

        for metric in self.metrics:
            series = data[metric].to_numpy(dtype=float)
            if weights is not None:
                threshold = _weighted_quantile(series, weights, self.quantile)
            else:
                threshold = float(np.quantile(series, self.quantile))
            if not self.higher_is_outlier:
                threshold = float(np.quantile(series, 1.0 - self.quantile)) if weights is None else _weighted_quantile(series, weights, 1.0 - self.quantile)
            self.thresholds[metric] = threshold

        self._mark_fitted()
        return self

    def score(self, data: pd.DataFrame) -> pd.Series:
        self._ensure_fitted()
        ensure_columns(data, self.metrics)

        def per_row(row: pd.Series) -> float:
            deltas: list[float] = []
            for metric in self.metrics:
                value = float(row[metric])
                threshold = self.thresholds[metric]
                if self.higher_is_outlier:
                    deltas.append(value - threshold)
                else:
                    deltas.append(threshold - value)
            return float(np.max(deltas))

        scores = data.apply(per_row, axis=1)
        return scores

    def predict(self, data: pd.DataFrame) -> pd.Series:
        scores = self.score(data)
        return scores > 0.0


class AggregatedArithmeticClassifier(Classifier):
    """Detect groups deviating from the global per-unit average."""

    def __init__(
        self,
        metrics: Sequence[str],
        *,
        count_column: str = "count",
        zscore_threshold: float = 2.0,
    ) -> None:
        if not metrics:
            raise ValueError("At least one metric column must be provided.")
        if zscore_threshold <= 0:
            raise ValueError("z-score threshold must be positive.")
        self.metrics = list(metrics)
        self.count_column = count_column
        self.zscore_threshold = float(zscore_threshold)
        self._means: dict[str, float] = {}
        self._stds: dict[str, float] = {}

    def _per_unit(self, data: pd.DataFrame) -> pd.DataFrame:
        ensure_columns(data, self.metrics)
        ensure_columns(data, [self.count_column])
        counts = data[self.count_column].to_numpy(dtype=float)
        if np.any(counts <= 0):
            raise ValueError("Counts must be positive for arithmetic classifier.")
        values = data[self.metrics].to_numpy(dtype=float)
        per_unit = values / counts[:, None]
        return pd.DataFrame(per_unit, columns=self.metrics, index=data.index)

    def fit(self, data: pd.DataFrame) -> "AggregatedArithmeticClassifier":
        per_unit = self._per_unit(data)
        counts = data[self.count_column].to_numpy(dtype=float)
        weights = counts / counts.sum()

        for metric in self.metrics:
            values = per_unit[metric].to_numpy(dtype=float)
            mean = float(np.dot(values, weights))
            variance = float(np.dot((values - mean) ** 2, weights))
            std = float(np.sqrt(variance)) if variance > 0 else 1e-9
            self._means[metric] = mean
            self._stds[metric] = std

        self._mark_fitted()
        return self

    def score(self, data: pd.DataFrame) -> pd.Series:
        self._ensure_fitted()
        per_unit = self._per_unit(data)

        def per_row(row: pd.Series) -> float:
            zscores: list[float] = []
            for metric in self.metrics:
                std = self._stds[metric]
                mean = self._means[metric]
                zscore = abs((float(row[metric]) - mean) / std)
                zscores.append(zscore - self.zscore_threshold)
            return float(np.max(zscores))

        scores = per_unit.apply(per_row, axis=1)
        return scores

    def predict(self, data: pd.DataFrame) -> pd.Series:
        scores = self.score(data)
        return scores > 0.0

