"""Configurable metric thresholds for python summarizers."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, Iterator, Mapping, MutableMapping, Optional, Tuple

from macrobase_py.metrics import ContingencyTable, compute_metrics


@dataclass(frozen=True)
class MetricThresholds:
    """Declarative specification of metric thresholds.

    The class exposes a :py:meth:`passes` helper that can be directly plugged
    into summarizers.  By default it understands the most common MacroBase
    metrics but it can also be extended with custom ``minimums`` and
    ``maximums`` dictionaries to support bespoke metrics.
    """

    min_support: Optional[float] = None
    min_outlier_support: Optional[float] = None
    min_inlier_support: Optional[float] = None
    min_risk_ratio: Optional[float] = None
    min_risk_difference: Optional[float] = None
    min_lift: Optional[float] = None
    min_leverage: Optional[float] = None
    maximums: Mapping[str, float] = field(default_factory=dict)
    minimums: Mapping[str, float] = field(default_factory=dict)

    def _iter_minimums(self) -> Iterator[Tuple[str, float]]:
        if self.min_support is not None:
            yield "support", self.min_support
        if self.min_outlier_support is not None:
            yield "outlier_support", self.min_outlier_support
        if self.min_inlier_support is not None:
            yield "inlier_support", self.min_inlier_support
        if self.min_risk_ratio is not None:
            yield "risk_ratio", self.min_risk_ratio
        if self.min_risk_difference is not None:
            yield "risk_difference", self.min_risk_difference
        if self.min_lift is not None:
            yield "lift", self.min_lift
        if self.min_leverage is not None:
            yield "leverage", self.min_leverage
        for metric, threshold in self.minimums.items():
            yield metric, threshold

    def _iter_maximums(self) -> Iterator[Tuple[str, float]]:
        for metric, threshold in self.maximums.items():
            yield metric, threshold

    def passes(
        self,
        table: ContingencyTable,
        metrics: Optional[Mapping[str, float]] = None,
    ) -> bool:
        """Return ``True`` if ``table`` satisfies all thresholds."""

        metrics = dict(metrics or compute_metrics(table))
        for metric, threshold in self._iter_minimums():
            value = metrics.get(metric)
            if value is None or value < threshold:
                return False
        for metric, threshold in self._iter_maximums():
            value = metrics.get(metric)
            if value is None or value > threshold:
                return False
        return True

    def failed_thresholds(
        self,
        table: ContingencyTable,
        metrics: Optional[Mapping[str, float]] = None,
    ) -> Dict[str, Tuple[Optional[float], float]]:
        """Return a mapping of metric name -> (value, threshold) for failures."""

        metrics = dict(metrics or compute_metrics(table))
        failures: Dict[str, Tuple[Optional[float], float]] = {}
        for metric, threshold in self._iter_minimums():
            value = metrics.get(metric)
            if value is None or value < threshold:
                failures[metric] = (value, threshold)
        for metric, threshold in self._iter_maximums():
            value = metrics.get(metric)
            if value is None or value > threshold:
                failures[metric] = (value, threshold)
        return failures


def filter_candidates(
    candidates: Iterable[Tuple[object, ContingencyTable]],
    thresholds: MetricThresholds,
    *,
    metrics_cache: Optional[MutableMapping[object, Mapping[str, float]]] = None,
) -> Iterator[Tuple[object, Mapping[str, float]]]:
    """Yield ``(candidate, metrics)`` pairs that satisfy ``thresholds``.

    ``candidates`` is expected to be an iterable of pairs where the first item
    is an arbitrary identifier and the second item is a
    :class:`~macrobase_py.metrics.ContingencyTable` describing the candidate's
    counts.  ``metrics_cache`` can be provided to reuse pre-computed metrics
    across multiple passes.
    """

    cache = metrics_cache if metrics_cache is not None else {}
    for candidate, table in candidates:
        metrics = cache.get(candidate)
        if metrics is None:
            metrics = compute_metrics(table)
            cache[candidate] = metrics
        if thresholds.passes(table, metrics):
            yield candidate, metrics
