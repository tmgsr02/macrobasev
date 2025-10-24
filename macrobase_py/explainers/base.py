"""Base classes shared by summarizers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import OrderedDict
from itertools import combinations
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from .explanations import Explanation

Combination = Tuple[Tuple[str, Any], ...]


class BatchSummarizer(ABC):
    """Base class for MacroBase-style summarizers operating on pandas data.

    Subclasses implement :meth:`_run` to generate candidate explanations. The
    base class handles data validation, ratio metric computation, filtering, and
    conversion into :class:`Explanation` objects ready for reporting.
    """

    def __init__(
        self,
        *,
        min_support: float = 0.1,
        min_ratio_metric: float = 1.0,
        ratio_metric: str = "risk_ratio",
        attributes: Optional[Sequence[str]] = None,
        outlier_column: str = "is_outlier",
        max_order: Optional[int] = None,
        top_k: Optional[int] = 20,
        functional_dependencies: Optional[Dict[str, Iterable[str]]] = None,
        use_fd_hints: bool = False,
    ) -> None:
        self.min_support = min_support
        self.min_ratio_metric = min_ratio_metric
        self.ratio_metric = ratio_metric
        self.attributes = list(attributes) if attributes is not None else None
        self.outlier_column = outlier_column
        self.max_order = max_order
        self.top_k = top_k
        self.functional_dependencies = (
            {
                determinant: frozenset(dependents)
                for determinant, dependents in (functional_dependencies or {}).items()
            }
        )
        self.use_fd_hints = use_fd_hints

        self._df: Optional[pd.DataFrame] = None
        self._outlier_mask: Optional[pd.Series] = None
        self._n_outliers: int = 0
        self._n_inliers: int = 0
        self._total_rows: int = 0
        self._attribute_values: Dict[str, List[Any]] = {}

    # ------------------------------------------------------------------
    # Fluent configuration helpers
    # ------------------------------------------------------------------
    def set_min_support(self, value: float) -> "BatchSummarizer":
        self.min_support = value
        return self

    def set_min_ratio_metric(self, value: float) -> "BatchSummarizer":
        self.min_ratio_metric = value
        return self

    def set_ratio_metric(self, metric: str) -> "BatchSummarizer":
        self.ratio_metric = metric
        return self

    def set_attributes(self, attributes: Sequence[str]) -> "BatchSummarizer":
        self.attributes = list(attributes)
        return self

    def set_outlier_column(self, column: str) -> "BatchSummarizer":
        self.outlier_column = column
        return self

    def set_max_order(self, max_order: int) -> "BatchSummarizer":
        self.max_order = max_order
        return self

    def set_top_k(self, top_k: Optional[int]) -> "BatchSummarizer":
        self.top_k = top_k
        return self

    def set_functional_dependencies(
        self, functional_dependencies: Dict[str, Iterable[str]]
    ) -> "BatchSummarizer":
        self.functional_dependencies = {
            determinant: frozenset(dependents)
            for determinant, dependents in functional_dependencies.items()
        }
        return self

    def enable_fd_hints(self, enabled: bool = True) -> "BatchSummarizer":
        self.use_fd_hints = enabled
        return self

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def explain(self, df: pd.DataFrame) -> List[Explanation]:
        """Generate explanations for the provided data frame."""

        working_df = df.copy()
        self._prepare(working_df)
        explanations = self._run(working_df)
        return self._finalize(explanations)

    # ------------------------------------------------------------------
    # Internal helpers shared by subclasses
    # ------------------------------------------------------------------
    def _prepare(self, df: pd.DataFrame) -> None:
        if self.outlier_column not in df.columns:
            raise KeyError(f"Outlier column '{self.outlier_column}' not found in frame")

        if self.attributes is None:
            self.attributes = [
                col
                for col in df.columns
                if col != self.outlier_column
            ]
        else:
            missing = [col for col in self.attributes if col not in df.columns]
            if missing:
                raise KeyError(f"Attributes missing from frame: {missing}")

        for attribute in self.attributes:
            if df[attribute].isnull().any():
                raise ValueError(
                    f"Attribute '{attribute}' contains null values which are not supported"
                )

        self._df = df
        self._outlier_mask = df[self.outlier_column].astype(bool)
        self._total_rows = len(df)
        self._n_outliers = int(self._outlier_mask.sum())
        self._n_inliers = self._total_rows - self._n_outliers
        if self._n_outliers == 0:
            raise ValueError("At least one outlier is required for summarization")
        if self._n_inliers == 0:
            raise ValueError("At least one inlier is required for summarization")

        self._attribute_values = {
            attr: sorted(df[attr].unique().tolist()) for attr in self.attributes
        }

        if self.max_order is None:
            self.max_order = len(self.attributes)

    def _finalize(self, explanations: List[Explanation]) -> List[Explanation]:
        metric_name = self.ratio_metric
        explanations = [
            exp for exp in explanations if self._metric_from_explanation(exp, metric_name) is not None
        ]

        explanations.sort(
            key=lambda exp: (
                self._metric_from_explanation(exp, metric_name) or -np.inf,
                exp.support_outliers,
                exp.risk_difference,
            ),
            reverse=True,
        )
        if self.top_k is not None:
            explanations = explanations[: self.top_k]
        return explanations

    def _metric_from_explanation(self, explanation: Explanation, metric_name: str) -> Optional[float]:
        if metric_name == "risk_ratio":
            return explanation.risk_ratio
        if metric_name == "risk_difference":
            return explanation.risk_difference
        if metric_name == "support_outliers":
            return explanation.support_outliers
        if metric_name == "support_total":
            return explanation.support_total
        raise ValueError(f"Unsupported ratio metric: {metric_name}")

    def _passes_thresholds(self, metrics: Dict[str, float]) -> bool:
        return self._has_min_support(metrics) and self._satisfies_ratio(metrics)

    def _metric_value(self, metrics: Dict[str, float]) -> Optional[float]:
        metric_name = self.ratio_metric
        if metric_name == "risk_ratio":
            return metrics["risk_ratio"]
        if metric_name == "risk_difference":
            return metrics["risk_difference"]
        if metric_name == "support_outliers":
            return metrics["support_outliers"]
        if metric_name == "support_total":
            return metrics["support_total"]
        raise ValueError(f"Unsupported ratio metric: {metric_name}")

    def _has_min_support(self, metrics: Dict[str, float]) -> bool:
        return metrics["support_outliers"] >= self.min_support

    def _satisfies_ratio(self, metrics: Dict[str, float]) -> bool:
        metric_value = self._metric_value(metrics)
        if metric_value is None:
            return False
        return metric_value >= self.min_ratio_metric

    def _compute_metrics(self, combination: Combination) -> Dict[str, float]:
        assert self._df is not None and self._outlier_mask is not None

        mask = pd.Series(True, index=self._df.index)
        for attribute, value in combination:
            mask &= self._df[attribute] == value

        outlier_mask = mask & self._outlier_mask
        inlier_mask = mask & (~self._outlier_mask)

        count_outliers = int(outlier_mask.sum())
        count_inliers = int(inlier_mask.sum())

        support_outliers = (
            count_outliers / self._n_outliers if self._n_outliers else 0.0
        )
        support_inliers = (
            count_inliers / self._n_inliers if self._n_inliers else 0.0
        )
        support_total = (count_outliers + count_inliers) / self._total_rows

        if support_inliers == 0.0:
            risk_ratio = float("inf") if support_outliers > 0 else 0.0
        else:
            risk_ratio = support_outliers / support_inliers
        risk_difference = support_outliers - support_inliers

        # Simple difference-of-proportions z-score as significance proxy.
        pooled = (count_outliers + count_inliers) / self._total_rows
        variance = pooled * (1 - pooled)
        denom = 0.0
        if self._n_outliers:
            denom += 1 / self._n_outliers
        if self._n_inliers:
            denom += 1 / self._n_inliers
        if variance == 0.0 or denom == 0.0:
            significance = 0.0
        else:
            significance = (risk_difference) / np.sqrt(variance * denom)

        return {
            "support_outliers": support_outliers,
            "support_inliers": support_inliers,
            "support_total": support_total,
            "risk_ratio": risk_ratio,
            "risk_difference": risk_difference,
            "significance": significance,
        }

    def _build_explanation(
        self, combination: Combination, metrics: Dict[str, float]
    ) -> Explanation:
        attribute_map = OrderedDict(combination)
        return Explanation(
            attribute_map,
            metrics["support_outliers"],
            metrics["support_inliers"],
            metrics["support_total"],
            metrics["risk_ratio"],
            metrics["risk_difference"],
            metrics["significance"],
        )

    def _is_valid_combination(self, combination: Combination) -> bool:
        if len(combination) > (self.max_order or len(self.attributes or [])):
            return False
        if self.use_fd_hints and self.functional_dependencies:
            attrs = {attr for attr, _ in combination}
            for determinant, dependents in self.functional_dependencies.items():
                if determinant in attrs and dependents & attrs:
                    return False
        return True

    @staticmethod
    def _normalize_combination(
        combination: Sequence[Tuple[str, Any]]
    ) -> Optional[Combination]:
        seen: Dict[str, Any] = {}
        for attribute, value in combination:
            if attribute in seen and seen[attribute] != value:
                return None
            seen[attribute] = value
        ordered = tuple(sorted(seen.items(), key=lambda item: item[0]))
        return ordered

    def _subsets_are_frequent(
        self,
        combination: Combination,
        frequent_subsets: Iterable[Combination],
    ) -> bool:
        frequent_set = set(frequent_subsets)
        k = len(combination)
        for subset in combinations(combination, k - 1):
            normalized = self._normalize_combination(subset)
            if normalized is None or normalized not in frequent_set:
                return False
        return True

    @abstractmethod
    def _run(self, df: pd.DataFrame) -> List[Explanation]:
        """Return all candidate explanations for ``df``."""


__all__ = ["BatchSummarizer", "Combination"]
