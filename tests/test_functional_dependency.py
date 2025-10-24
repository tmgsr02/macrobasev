from __future__ import annotations

import random
from typing import Iterable

from macrobase_py.metrics import BinaryGroup, ContingencyTable
from macrobase_py.summarizers import MetricThresholds, filter_candidates
from macrobase_py.utils import (
    FunctionalDependencyResult,
    check_functional_dependency,
    dependency_summary,
)


def generate_dependency_records(
    *, seed: int, size: int = 30
) -> Iterable[dict[str, int]]:
    rng = random.Random(seed)
    mapping: dict[int, int] = {}
    for _ in range(size):
        determinant = rng.randint(0, 10)
        if determinant not in mapping:
            mapping[determinant] = rng.randint(0, 20)
        yield {"det": determinant, "dep": mapping[determinant], "noise": rng.randint(0, 50)}


def test_functional_dependency_holds() -> None:
    records = list(generate_dependency_records(seed=1))
    result = check_functional_dependency(records, ["det"], ["dep"])
    assert isinstance(result, FunctionalDependencyResult)
    assert result.holds
    assert result.violating_rows == 0


def test_functional_dependency_detects_violations() -> None:
    records = list(generate_dependency_records(seed=2))
    records.append({"det": records[0]["det"], "dep": records[0]["dep"] + 1, "noise": 0})
    result = check_functional_dependency(records, ["det"], ["dep"])
    assert not result.holds
    assert result.violating_rows >= 2


def test_dependency_summary_thresholds() -> None:
    holds = FunctionalDependencyResult(("a",), ("b",), 10, 0, {})
    violates = FunctionalDependencyResult(("c",), ("d",), 10, 5, {("x",): (("y",), ("z",))})
    summary = dependency_summary([holds, violates], max_ratio=0.2)
    assert summary["a -> b"]
    assert not summary["c -> d"]


def test_filter_candidates_integration() -> None:
    table = ContingencyTable(BinaryGroup(5, 5), BinaryGroup(5, 5))
    thresholds = MetricThresholds(min_risk_ratio=1.0)
    candidates = [("candidate", table)]
    filtered = list(filter_candidates(candidates, thresholds))
    assert filtered
    candidate, metrics = filtered[0]
    assert candidate == "candidate"
    assert metrics["risk_ratio"] >= 1.0
