"""Heuristic search summarizer for high-dimensional spaces."""

from __future__ import annotations

import heapq
from typing import Dict, List, Sequence, Set, Tuple

import pandas as pd

from .base import BatchSummarizer, Combination
from .explanations import Explanation


class HeuristicSummarizer(BatchSummarizer):
    """Beam-search summarizer that favors combinations with high ratios."""

    def __init__(self, *args, beam_width: int = 10, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.beam_width = beam_width

    def set_beam_width(self, width: int) -> "HeuristicSummarizer":
        self.beam_width = width
        return self

    def _run(self, df: pd.DataFrame) -> List[Explanation]:
        explanations: List[Explanation] = []
        candidate_heap: List[Tuple[float, Combination, Dict[str, float]]] = []

        # Seed with singletons.
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
                score = self._metric_value(metrics)
                heapq.heappush(candidate_heap, (-(score or 0.0), combination, metrics))

        visited: Set[Combination] = {item[1] for item in candidate_heap}

        current_depth = 1
        while candidate_heap and current_depth < (self.max_order or len(self.attributes or [])):
            next_candidates: List[Tuple[float, Combination, Dict[str, float]]] = []
            for _ in range(min(self.beam_width, len(candidate_heap))):
                score, base_combination, base_metrics = heapq.heappop(candidate_heap)
                next_candidates.append((score, base_combination, base_metrics))

            candidate_heap.clear()
            expansions: List[Tuple[float, Combination, Dict[str, float]]] = []
            for _, combination, _ in next_candidates:
                present_attrs = {attr for attr, _ in combination}
                for attribute in self.attributes or []:
                    if attribute in present_attrs:
                        continue
                    for value in self._attribute_values[attribute]:
                        new_comb = self._normalize_combination(combination + ((attribute, value),))
                        if new_comb is None:
                            continue
                        if new_comb in visited:
                            continue
                        if not self._is_valid_combination(new_comb):
                            continue
                        metrics = self._compute_metrics(new_comb)
                        if not self._has_min_support(metrics):
                            continue
                        if self._satisfies_ratio(metrics):
                            explanation = self._build_explanation(new_comb, metrics)
                            explanations.append(explanation)
                        score_value = self._metric_value(metrics) or 0.0
                        heapq.heappush(expansions, (-(score_value), new_comb, metrics))
                        visited.add(new_comb)

            # Prepare next level beam.
            for _ in range(min(self.beam_width, len(expansions))):
                heap_item = heapq.heappop(expansions)
                heapq.heappush(candidate_heap, heap_item)

            current_depth += 1

        return explanations


__all__ = ["HeuristicSummarizer"]
