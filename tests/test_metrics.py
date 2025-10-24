from __future__ import annotations

import math
import random
from typing import Iterator

import pytest

from macrobase_py.metrics import (
    BinaryGroup,
    ContingencyTable,
    compute_metrics,
    leverage,
    lift,
    risk,
    risk_difference,
    risk_ratio,
    support,
)
from macrobase_py.summarizers import MetricThresholds


def random_tables(seed: int = 0, cases: int = 200) -> Iterator[ContingencyTable]:
    rng = random.Random(seed)
    for _ in range(cases):
        total_outliers = rng.randint(0, 500)
        total_inliers = rng.randint(0, 500)
        exposed_outliers = rng.randint(0, total_outliers) if total_outliers else 0
        exposed_inliers = rng.randint(0, total_inliers) if total_inliers else 0
        unexposed_outliers = total_outliers - exposed_outliers
        unexposed_inliers = total_inliers - exposed_inliers
        yield ContingencyTable(
            exposed=BinaryGroup(outliers=exposed_outliers, inliers=exposed_inliers),
            unexposed=BinaryGroup(outliers=unexposed_outliers, inliers=unexposed_inliers),
        )


def test_support_matches_definition() -> None:
    for table in random_tables(seed=1):
        if table.population == 0:
            assert support(table) == 0
        else:
            expected = table.total_exposed / table.population
            assert support(table) == pytest.approx(expected)


def test_risk_ratio_matches_definition() -> None:
    for table in random_tables(seed=2):
        result = risk_ratio(table)
        exposed_total = table.total_exposed
        unexposed_total = table.total_unexposed

        if exposed_total == 0 or unexposed_total == 0:
            assert result == 0.0
            continue

        if table.unexposed.outliers == 0:
            assert result == math.inf
        else:
            expected = (table.exposed.outliers / exposed_total) / (
                table.unexposed.outliers / unexposed_total
            )
            assert result == pytest.approx(expected)


def test_risk_difference_matches_definition() -> None:
    for table in random_tables(seed=3):
        result = risk_difference(table)
        exposed_total = table.total_exposed
        unexposed_total = table.total_unexposed

        if exposed_total == 0 or unexposed_total == 0:
            assert result == 0.0
            continue

        expected = (table.exposed.outliers / exposed_total) - (
            table.unexposed.outliers / unexposed_total
        )
        assert result == pytest.approx(expected)


def test_lift_matches_definition() -> None:
    for table in random_tables(seed=4):
        result = lift(table)
        if table.population == 0 or table.total_exposed == 0 or table.total_outliers == 0:
            assert result == 0.0
        else:
            expected = (table.exposed.outliers / table.total_exposed) / (
                table.total_outliers / table.population
            )
            assert result == pytest.approx(expected)


def test_leverage_matches_definition() -> None:
    for table in random_tables(seed=5):
        result = leverage(table)
        if table.population == 0:
            assert result == 0.0
        else:
            expected = (table.exposed.outliers / table.population) - (
                (table.total_exposed / table.population)
                * (table.total_outliers / table.population)
            )
            assert result == pytest.approx(expected)


def test_metric_thresholds_respect_bounds() -> None:
    for table in random_tables(seed=6):
        metrics = compute_metrics(table)
        thresholds = MetricThresholds(min_support=max(metrics["support"] - 1e-9, 0.0))
        assert thresholds.passes(table, metrics)

        failure_threshold = MetricThresholds(min_support=metrics["support"] + 1e-6)
        if math.isfinite(metrics["support"]):
            assert not failure_threshold.passes(table, metrics)


def test_metric_thresholds_failed_thresholds() -> None:
    for table in random_tables(seed=7):
        metrics = compute_metrics(table)
        threshold_value = metrics["risk"] + 0.5
        thresholds = MetricThresholds(minimums={"risk": threshold_value})
        failures = thresholds.failed_thresholds(table, metrics)
        assert "risk" in failures
        assert failures["risk"][1] == threshold_value
