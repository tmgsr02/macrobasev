"""Mean shift inspired density classifiers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from .base import Classifier, ensure_columns, to_series


@dataclass
class _GaussianBandwidth:
    value: np.ndarray


class _GaussianKDE:
    """Lightweight Gaussian KDE supporting optional sample weights."""

    def __init__(self, bandwidth: float | Sequence[float] | None = None) -> None:
        self.bandwidth = bandwidth
        self.samples: np.ndarray | None = None
        self.weights: np.ndarray | None = None
        self._bw: _GaussianBandwidth | None = None

    def fit(self, samples: np.ndarray, weights: np.ndarray | None = None) -> "_GaussianKDE":
        if samples.ndim != 2:
            raise ValueError("Samples must be a 2D array.")
        self.samples = np.asarray(samples, dtype=float)
        self.weights = None if weights is None else np.asarray(weights, dtype=float)
        self._bw = _GaussianBandwidth(self._compute_bandwidth())
        return self

    def score_samples(self, samples: np.ndarray) -> np.ndarray:
        if self.samples is None or self._bw is None:
            raise RuntimeError("KDE must be fitted before scoring samples.")

        query = np.asarray(samples, dtype=float)
        diffs = (query[:, None, :] - self.samples[None, :, :]) / self._bw.value
        exponent = -0.5 * np.sum(diffs * diffs, axis=2)
        kernel_vals = np.exp(exponent)

        if self.weights is not None:
            weights = self.weights / self.weights.sum()
            density = np.dot(kernel_vals, weights)
        else:
            density = kernel_vals.mean(axis=1)

        norm = np.prod(self._bw.value) * np.power(2.0 * np.pi, self.samples.shape[1] / 2.0)
        density = density / norm
        return np.log(density + 1e-12)

    def _compute_bandwidth(self) -> np.ndarray:
        assert self.samples is not None
        n, d = self.samples.shape
        if isinstance(self.bandwidth, Sequence):
            bw = np.asarray(list(self.bandwidth), dtype=float)
            if bw.shape != (d,):
                raise ValueError("Bandwidth sequence must have one value per feature.")
            return np.where(bw > 0, bw, 1.0)
        if isinstance(self.bandwidth, (int, float)) and self.bandwidth is not None:
            bw_value = float(self.bandwidth)
            if bw_value <= 0:
                raise ValueError("Bandwidth must be positive.")
            return np.full(d, bw_value)

        std = self.samples.std(axis=0, ddof=1)
        std = np.where(std > 0, std, 1.0)
        factor = np.power(n, -1.0 / (d + 4))
        return std * factor


class MeanShiftClassifier(Classifier):
    """Density based outlier detector using a Gaussian KDE approximation."""

    def __init__(
        self,
        metrics: Sequence[str],
        *,
        bandwidth: float | Sequence[float] | None = None,
        contamination: float = 0.05,
    ) -> None:
        if not metrics:
            raise ValueError("At least one metric column must be provided.")
        if not (0.0 < contamination < 1.0):
            raise ValueError("Contamination must be within (0, 1).")

        self.metrics = list(metrics)
        self.bandwidth = bandwidth
        self.contamination = contamination
        self._kde = _GaussianKDE(bandwidth)
        self.threshold: float | None = None

    def _values(self, data: pd.DataFrame) -> np.ndarray:
        ensure_columns(data, self.metrics)
        return data[self.metrics].to_numpy(dtype=float)

    def fit(self, data: pd.DataFrame) -> "MeanShiftClassifier":
        samples = self._values(data)
        self._kde.fit(samples)
        log_density = self._kde.score_samples(samples)
        self.threshold = float(np.quantile(log_density, self.contamination))
        self._mark_fitted()
        return self

    def score(self, data: pd.DataFrame) -> pd.Series:
        self._ensure_fitted()
        assert self.threshold is not None
        samples = self._values(data)
        log_density = self._kde.score_samples(samples)
        scores = self.threshold - log_density
        return to_series(scores, data.index)

    def predict(self, data: pd.DataFrame) -> pd.Series:
        self._ensure_fitted()
        assert self.threshold is not None
        samples = self._values(data)
        log_density = self._kde.score_samples(samples)
        return to_series(log_density < self.threshold, data.index)


class CountMeanShiftClassifier(MeanShiftClassifier):
    """Mean shift variant that accounts for aggregated counts."""

    def __init__(
        self,
        metrics: Sequence[str],
        *,
        count_column: str = "count",
        bandwidth: float | Sequence[float] | None = None,
        contamination: float = 0.05,
    ) -> None:
        super().__init__(metrics, bandwidth=bandwidth, contamination=contamination)
        self.count_column = count_column

    def _expanded_values(self, data: pd.DataFrame) -> np.ndarray:
        ensure_columns(data, [self.count_column])
        values = self._values(data)
        counts = data[self.count_column].to_numpy(dtype=int)
        if np.any(counts < 0):
            raise ValueError("Counts must be non-negative.")
        expanded: list[np.ndarray] = []
        for value, count in zip(values, counts, strict=False):
            if count == 0:
                continue
            expanded.extend([value] * int(count))
        if not expanded:
            raise ValueError("Aggregated data does not contain any observations.")
        return np.vstack(expanded)

    def fit(self, data: pd.DataFrame) -> "CountMeanShiftClassifier":
        expanded = self._expanded_values(data)
        self._kde.fit(expanded)
        log_density = self._kde.score_samples(expanded)
        self.threshold = float(np.quantile(log_density, self.contamination))
        self._mark_fitted()
        return self

