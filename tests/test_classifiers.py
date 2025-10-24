import numpy as np
import pandas as pd

from macrobase_py.classifiers import (
    AggregatedArithmeticClassifier,
    AggregatedQuantileClassifier,
    CountMeanShiftClassifier,
    MeanShiftClassifier,
    PercentileOutlierClassifier,
    PredicateClassifier,
)


def test_percentile_classifier_flags_high_values():
    data = pd.DataFrame({"metric": [1, 2, 3, 4, 100]})
    clf = PercentileOutlierClassifier(metrics=["metric"], percentile=80.0)
    clf.fit(data)
    scores = clf.score(data)
    preds = clf.predict(data)
    assert preds.iloc[-1]
    assert scores.iloc[-1] > 0
    assert not preds.iloc[0]
    assert scores.iloc[0] <= 0


def test_predicate_classifier_evaluates_expression():
    data = pd.DataFrame({"value": [1, 6, 10], "flag": [0, 1, 1]})
    clf = PredicateClassifier("value > 5 and flag == 1")
    clf.fit(data)
    preds = clf.predict(data)
    scores = clf.score(data)
    assert preds.tolist() == [False, True, True]
    assert scores.tolist() == [0.0, 1.0, 1.0]


def test_mean_shift_classifier_detects_outliers():
    rng = np.random.default_rng(0)
    normal = rng.normal(0, 1, size=(200, 2))
    outliers = np.array([[5.0, 5.0], [6.0, 6.0]])
    data = pd.DataFrame(np.vstack([normal, outliers]), columns=["x", "y"])

    clf = MeanShiftClassifier(metrics=["x", "y"], contamination=0.02, bandwidth=1.0)
    clf.fit(data)
    preds = clf.predict(data)
    scores = clf.score(data)

    assert preds.iloc[-1]
    assert preds.iloc[-2]
    assert scores.iloc[-1] > 0
    assert preds.sum() >= 2


def test_count_mean_shift_classifier_uses_counts():
    data = pd.DataFrame(
        {
            "x": [0.0, 0.1, 5.0],
            "y": [0.0, -0.1, 5.0],
            "count": [200, 180, 2],
        }
    )

    clf = CountMeanShiftClassifier(metrics=["x", "y"], count_column="count", bandwidth=0.5, contamination=0.05)
    clf.fit(data)
    preds = clf.predict(data)
    scores = clf.score(data)

    assert preds.iloc[-1]
    assert scores.iloc[-1] > 0
    assert not preds.iloc[0]


def test_aggregated_quantile_classifier_flags_high_group():
    data = pd.DataFrame(
        {
            "group": list("ABCD"),
            "count": [20, 25, 22, 5],
            "metric": [1.0, 1.2, 0.9, 5.0],
        }
    )

    clf = AggregatedQuantileClassifier(metrics=["metric"], quantile=0.9, higher_is_outlier=True, count_column="count")
    clf.fit(data)
    preds = clf.predict(data)
    scores = clf.score(data)

    assert preds.iloc[-1]
    assert scores.iloc[-1] > 0
    assert not preds.iloc[0]


def test_aggregated_arithmetic_classifier_detects_ratio_shift():
    data = pd.DataFrame(
        {
            "group": list("ABC"),
            "count": [50, 60, 5],
            "metric": [100.0, 120.0, 70.0],
        }
    )

    clf = AggregatedArithmeticClassifier(metrics=["metric"], count_column="count", zscore_threshold=2.0)
    clf.fit(data)
    preds = clf.predict(data)
    scores = clf.score(data)

    assert preds.iloc[-1]
    assert scores.iloc[-1] > 0
    assert not preds.iloc[0]

