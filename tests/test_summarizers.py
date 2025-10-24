import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from macrobase_py.explainers import AprioriSummarizer, HeuristicSummarizer


def build_sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"region": "US", "browser": "Chrome", "segment": "A", "zip": "94107", "is_outlier": 1},
            {"region": "US", "browser": "Chrome", "segment": "A", "zip": "94107", "is_outlier": 1},
            {"region": "US", "browser": "Chrome", "segment": "B", "zip": "94107", "is_outlier": 1},
            {"region": "US", "browser": "Firefox", "segment": "B", "zip": "94107", "is_outlier": 1},
            {"region": "US", "browser": "Chrome", "segment": "A", "zip": "94107", "is_outlier": 0},
            {"region": "US", "browser": "Chrome", "segment": "B", "zip": "94107", "is_outlier": 0},
            {"region": "US", "browser": "Safari", "segment": "A", "zip": "94107", "is_outlier": 0},
            {"region": "US", "browser": "Firefox", "segment": "B", "zip": "94107", "is_outlier": 0},
            {"region": "EU", "browser": "Chrome", "segment": "A", "zip": "75001", "is_outlier": 0},
            {"region": "EU", "browser": "Chrome", "segment": "B", "zip": "75001", "is_outlier": 0},
            {"region": "EU", "browser": "Firefox", "segment": "B", "zip": "75001", "is_outlier": 0},
            {"region": "ASIA", "browser": "Chrome", "segment": "A", "zip": "100-0001", "is_outlier": 0},
            {"region": "ASIA", "browser": "Chrome", "segment": "B", "zip": "100-0001", "is_outlier": 0},
            {"region": "ASIA", "browser": "Safari", "segment": "B", "zip": "100-0001", "is_outlier": 0},
        ]
    )


def test_apriori_summarizer_support_and_ratio():
    df = build_sample_frame()
    summarizer = (
        AprioriSummarizer(min_support=0.2, min_ratio_metric=2.0, max_order=3)
        .set_ratio_metric("risk_ratio")
        .enable_fd_hints(True)
        .set_functional_dependencies({"region": ["zip"]})
    )

    explanations = summarizer.explain(df)
    assert explanations, "Expected at least one explanation"

    top = explanations[0]
    assert dict(top.attributes) == {
        "browser": "Chrome",
        "region": "US",
        "segment": "A",
    }
    assert top.support_outliers == pytest.approx(0.5)
    assert top.support_inliers == pytest.approx(0.1)
    assert top.risk_ratio == pytest.approx(5.0)
    assert top.risk_difference == pytest.approx(0.4)
    assert top.significance > 0

    report = top.to_report_dict()
    assert report["risk_ratio"] == "5.00"
    assert report["outlier_support"].endswith("%")
    # Functional dependency hint should prevent redundant attribute usage.
    for explanation in explanations:
        assert "region" not in explanation.attributes or "zip" not in explanation.attributes

    assert any(
        dict(exp.attributes) == {"browser": "Chrome", "region": "US"}
        for exp in explanations
    )


def test_heuristic_matches_top_combo():
    df = build_sample_frame()
    summarizer = HeuristicSummarizer(
        min_support=0.2,
        min_ratio_metric=2.0,
        max_order=3,
        beam_width=5,
    ).set_ratio_metric("risk_ratio")

    explanations = summarizer.explain(df)
    assert explanations, "Expected at least one explanation"

    top = explanations[0]
    assert dict(top.attributes) == {
        "browser": "Chrome",
        "region": "US",
        "segment": "A",
    }
    assert top.risk_ratio == pytest.approx(5.0)
    assert top.support_outliers == pytest.approx(0.5)

    # Ensure report formatting is accessible for business users.
    report = top.to_report_dict()
    assert report["outlier_support"].startswith("50.0")
    assert report["risk_difference"].startswith("40.0")


def test_invalid_inputs_raise():
    df = build_sample_frame()
    summarizer = AprioriSummarizer()

    with pytest.raises(KeyError):
        summarizer.set_attributes(["does_not_exist"]).explain(df)

    all_outlier = df.assign(is_outlier=1)
    with pytest.raises(ValueError):
        AprioriSummarizer().explain(all_outlier)

    all_inlier = df.assign(is_outlier=0)
    with pytest.raises(ValueError):
        AprioriSummarizer().explain(all_inlier)
