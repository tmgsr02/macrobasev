"""Explainability primitives for MacroBase-inspired workflows."""

from .base import BatchSummarizer
from .apriori import AprioriSummarizer
from .heuristic import HeuristicSummarizer
from .explanations import Explanation

__all__ = [
    "BatchSummarizer",
    "AprioriSummarizer",
    "HeuristicSummarizer",
    "Explanation",
]
"""Module placeholder for macrobase_py.explainers."""

__all__: list[str] = []
