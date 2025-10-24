"""Classifier evaluating boolean predicates over dataframe columns."""

from __future__ import annotations

import pandas as pd

from .base import Classifier


class PredicateClassifier(Classifier):
    """Evaluate a boolean predicate to classify rows."""

    def __init__(self, predicate: str) -> None:
        if not predicate:
            raise ValueError("A predicate expression must be provided.")
        self.predicate = predicate

    def fit(self, data: pd.DataFrame) -> "PredicateClassifier":  # noqa: D401
        """Predicate classifiers do not require fitting."""

        self._mark_fitted()
        return self

    def score(self, data: pd.DataFrame) -> pd.Series:
        self._ensure_fitted()
        mask = data.eval(self.predicate)
        if mask.dtype != bool:
            mask = mask.astype(bool)
        return mask.astype(float)

    def predict(self, data: pd.DataFrame) -> pd.Series:
        scores = self.score(data)
        return scores.astype(bool)

