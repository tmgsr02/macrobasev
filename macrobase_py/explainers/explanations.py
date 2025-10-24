"""Explanation objects returned by summarizers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Any, OrderedDict as OrderedDictType


@dataclass(order=True)
class Explanation:
    """Container for summarizer output prepared for reporting.

    The ``sort_index`` attribute allows explanations to be sorted by their
    utility metric while retaining all rich metadata for downstream
    consumption.
    """

    sort_index: float = field(init=False, repr=False, compare=True)
    attributes: "OrderedDictType[str, Any]"
    support_outliers: float
    support_inliers: float
    support_total: float
    risk_ratio: float
    risk_difference: float
    significance: float

    def __post_init__(self) -> None:
        # Higher is better, so we store the negative value because ``dataclass``
        # ordering compares ``sort_index`` directly.
        primary_metric = self.risk_ratio if self.risk_ratio != float("inf") else 1e12
        self.sort_index = -primary_metric

    def to_report_dict(self) -> Dict[str, Any]:
        """Return a human-readable mapping ready for business reports."""

        def _fmt_percent(value: float) -> str:
            return f"{value * 100:.1f}%"

        rr = "∞" if self.risk_ratio == float("inf") else f"{self.risk_ratio:.2f}"
        return {
            "attributes": dict(self.attributes),
            "outlier_support": _fmt_percent(self.support_outliers),
            "inlier_support": _fmt_percent(self.support_inliers),
            "overall_support": _fmt_percent(self.support_total),
            "risk_ratio": rr,
            "risk_difference": _fmt_percent(self.risk_difference),
            "significance_z": f"{self.significance:.2f}",
        }

    def __str__(self) -> str:
        parts = [f"{k}={v}" for k, v in self.attributes.items()]
        return (
            f"Explanation({', '.join(parts)}; "
            f"outlier_support={self.support_outliers:.3f}, "
            f"inlier_support={self.support_inliers:.3f}, "
            f"risk_ratio={'∞' if self.risk_ratio == float('inf') else f'{self.risk_ratio:.2f}'}, "
            f"risk_difference={self.risk_difference:.3f}, "
            f"significance_z={self.significance:.2f})"
        )
