"""Utility functions for computing summarization metrics.

These metrics are based on 2x2 contingency tables that compare the
frequency of a pattern among outliers ("exposed") to the rest of the data
("unexposed").  The helper dataclasses below provide a light-weight wrapper
around the required counts and perform basic validation so callers do not
need to repeatedly re-implement the same bookkeeping logic.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, MutableMapping, Optional

import math


@dataclass(frozen=True)
class BinaryGroup:
    """Container for positive/negative counts within a single group.

    The counts can either represent raw integers or aggregate weights.  They
    only need to be non-negative and finite.  The :class:`ContingencyTable`
    class combines two ``BinaryGroup`` instances to build a full 2x2 table.
    """

    outliers: float
    inliers: float

    def __post_init__(self) -> None:  # pragma: no cover - exercised indirectly
        if self.outliers < 0 or self.inliers < 0:
            raise ValueError("Counts must be non-negative")
        if not math.isfinite(self.outliers) or not math.isfinite(self.inliers):
            raise ValueError("Counts must be finite")

    @property
    def total(self) -> float:
        return self.outliers + self.inliers


@dataclass(frozen=True)
class ContingencyTable:
    """Representation of a 2x2 contingency table.

    ``exposed`` corresponds to the rows with the attribute/pattern we are
    evaluating, whereas ``unexposed`` captures the complement of that pattern.
    ``total`` describes the global population counts; it is optional and will
    be inferred when omitted.
    """

    exposed: BinaryGroup
    unexposed: BinaryGroup
    total: Optional[BinaryGroup] = None

    def __post_init__(self) -> None:  # pragma: no cover - exercised indirectly
        if self.total is not None:
            if self.total.outliers + self.total.inliers == 0:
                return
            if self.exposed.outliers > self.total.outliers:
                raise ValueError("Exposed outliers cannot exceed total outliers")
            if self.exposed.inliers > self.total.inliers:
                raise ValueError("Exposed inliers cannot exceed total inliers")
            if self.unexposed.outliers > self.total.outliers:
                raise ValueError("Unexposed outliers cannot exceed total outliers")
            if self.unexposed.inliers > self.total.inliers:
                raise ValueError("Unexposed inliers cannot exceed total inliers")
            total_outliers = self.exposed.outliers + self.unexposed.outliers
            total_inliers = self.exposed.inliers + self.unexposed.inliers
            if total_outliers > self.total.outliers + 1e-12:
                raise ValueError("Exposed+unexposed outliers exceed total outliers")
            if total_inliers > self.total.inliers + 1e-12:
                raise ValueError("Exposed+unexposed inliers exceed total inliers")

    @property
    def total_outliers(self) -> float:
        if self.total is not None:
            return self.total.outliers
        return self.exposed.outliers + self.unexposed.outliers

    @property
    def total_inliers(self) -> float:
        if self.total is not None:
            return self.total.inliers
        return self.exposed.inliers + self.unexposed.inliers

    @property
    def population(self) -> float:
        return self.total_outliers + self.total_inliers

    @property
    def total_exposed(self) -> float:
        return self.exposed.total

    @property
    def total_unexposed(self) -> float:
        return self.unexposed.total

    def as_dict(self) -> Dict[str, float]:
        return {
            "exposed_outliers": self.exposed.outliers,
            "exposed_inliers": self.exposed.inliers,
            "unexposed_outliers": self.unexposed.outliers,
            "unexposed_inliers": self.unexposed.inliers,
            "total_outliers": self.total_outliers,
            "total_inliers": self.total_inliers,
            "population": self.population,
        }


def _safe_ratio(numerator: float, denominator: float, *, zero: float = 0.0) -> float:
    if denominator == 0:
        return zero
    return numerator / denominator


def support(table: ContingencyTable) -> float:
    """Return the overall support of the exposed pattern.

    Support measures the fraction of the total population that exhibits the
    pattern under inspection ("exposed").  A value close to zero indicates a
    rare pattern whereas values close to one indicate a ubiquitous pattern.
    """

    return _safe_ratio(table.total_exposed, table.population)


def outlier_support(table: ContingencyTable) -> float:
    """Support restricted to outliers."""

    return _safe_ratio(table.exposed.outliers, table.total_outliers)


def inlier_support(table: ContingencyTable) -> float:
    """Support restricted to inliers."""

    return _safe_ratio(table.exposed.inliers, table.total_inliers)


def risk(table: ContingencyTable) -> float:
    """Probability of being an outlier among the exposed population."""

    return _safe_ratio(table.exposed.outliers, table.total_exposed)


def baseline_risk(table: ContingencyTable) -> float:
    """Probability of being an outlier among the unexposed population."""

    return _safe_ratio(
        table.unexposed.outliers,
        table.total_unexposed,
    )


def risk_ratio(table: ContingencyTable) -> float:
    """Return the risk ratio (a.k.a. relative risk).

    The computation mirrors the behaviour of the historical MacroBase Java
    implementation: if either exposed or unexposed populations are empty, the
    ratio is defined to be ``0``.  Conversely, if the unexposed population has
    zero outliers the ratio diverges to ``+inf`` because the denominator of the
    ratio approaches zero.
    """

    exposed_total = table.total_exposed
    unexposed_total = table.total_unexposed

    if exposed_total == 0 or unexposed_total == 0:
        return 0.0

    unexposed_outliers = table.unexposed.outliers
    if unexposed_outliers == 0:
        return math.inf

    return (table.exposed.outliers / exposed_total) / (
        unexposed_outliers / unexposed_total
    )


def risk_difference(table: ContingencyTable) -> float:
    """Return the absolute difference between exposed and baseline risk."""

    exposed_total = table.total_exposed
    unexposed_total = table.total_unexposed

    if exposed_total == 0 or unexposed_total == 0:
        return 0.0

    return (table.exposed.outliers / exposed_total) - (
        table.unexposed.outliers / unexposed_total
    )


def lift(table: ContingencyTable) -> float:
    """Return the lift of the rule ``exposed -> outlier``.

    Lift compares the probability of the consequent (being an outlier) given
    the antecedent (exposure) to the unconditional probability of the
    consequent.  Values greater than 1 indicate positive association.
    """

    population = table.population
    exposed_total = table.total_exposed
    if population == 0 or exposed_total == 0 or table.total_outliers == 0:
        return 0.0

    p_outlier_given_exposed = table.exposed.outliers / exposed_total
    p_outlier = table.total_outliers / population

    return p_outlier_given_exposed / p_outlier


def leverage(table: ContingencyTable) -> float:
    """Return the leverage of the rule ``exposed -> outlier``.

    Leverage measures the departure of the joint probability from what would
    be expected if exposure and being an outlier were independent.
    """

    population = table.population
    if population == 0:
        return 0.0

    p_exposed_and_outlier = table.exposed.outliers / population
    p_exposed = table.total_exposed / population
    p_outlier = table.total_outliers / population

    return p_exposed_and_outlier - (p_exposed * p_outlier)


def compute_metrics(table: ContingencyTable) -> Dict[str, float]:
    """Compute a dictionary of all supported metrics for ``table``."""

    return {
        "support": support(table),
        "outlier_support": outlier_support(table),
        "inlier_support": inlier_support(table),
        "risk": risk(table),
        "baseline_risk": baseline_risk(table),
        "risk_ratio": risk_ratio(table),
        "risk_difference": risk_difference(table),
        "lift": lift(table),
        "leverage": leverage(table),
    }


def update_metric_summary(
    summary: MutableMapping[str, float],
    metrics: Mapping[str, float],
    *,
    weight: float = 1.0,
) -> None:
    """Accumulate ``metrics`` into ``summary`` with the provided ``weight``.

    This helper is convenient when summarizers want to compute running
    aggregates of metric values while processing multiple candidate patterns.
    The update is performed in-place to avoid additional allocations.
    """

    for key, value in metrics.items():
        summary[key] = summary.get(key, 0.0) + value * weight
