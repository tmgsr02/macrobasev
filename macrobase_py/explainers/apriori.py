"""Apriori-style summarizer implementation."""

from __future__ import annotations

from itertools import combinations
from typing import Dict, List, Sequence, Set, Tuple

import pandas as pd

from .base import BatchSummarizer, Combination
from .explanations import Explanation


class AprioriSummarizer(BatchSummarizer):
    """Enumerate attribute combinations with classic Apriori pruning."""

    def _run(self, df: pd.DataFrame) -> List[Explanation]:
        explanations: List[Explanation] = []

        # Initialize with frequent singletons.
        level_itemsets: List[Tuple[Combination, Dict[str, float]]] = []
        for attribute in self.attributes or []:
            for value in self._attribute_values[attribute]:
                combination = self._normalize_combination(((attribute, value),))
                if combination is None or not self._is_valid_combination(combination):
                    continue
                metrics = self._compute_metrics(combination)
                if not self._has_min_support(metrics):
                    continue
                if self._satisfies_ratio(metrics):
                    explanation = self._build_explanation(combination, metrics)
                    explanations.append(explanation)
                level_itemsets.append((combination, metrics))

        k = 2
        while level_itemsets and k <= (self.max_order or len(self.attributes or [])):
            candidates = self._generate_candidates([combo for combo, _ in level_itemsets], k)
            next_level: List[Tuple[Combination, Dict[str, float]]] = []
            for combination in candidates:
                metrics = self._compute_metrics(combination)
                if not self._has_min_support(metrics):
                    continue
                if self._satisfies_ratio(metrics):
                    explanation = self._build_explanation(combination, metrics)
                    explanations.append(explanation)
                next_level.append((combination, metrics))
            level_itemsets = next_level
            k += 1

        return explanations

    def _generate_candidates(
        self, previous_level: Sequence[Combination], k: int
    ) -> List[Combination]:
        candidates: Set[Combination] = set()
        previous_set = set(previous_level)
        for i in range(len(previous_level)):
            for j in range(i + 1, len(previous_level)):
                left = previous_level[i]
                right = previous_level[j]
                if left[:-1] != right[:-1]:
                    continue
                merged = self._normalize_combination(left + right[-1:])
                if merged is None:
                    continue
                if not self._is_valid_combination(merged):
                    continue
                if not self._subsets_are_frequent(merged, previous_set):
                    continue
                candidates.add(merged)
        return sorted(candidates)


__all__ = ["AprioriSummarizer"]
