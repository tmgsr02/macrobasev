"""Concrete pipeline implementations for MacroBase Python."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence, Tuple, Type, Union

from .base import Operator, Transformer
from .config import BatchPipelineConfig, ClassifierConfig, CubePipelineConfig, SummarizerConfig

Record = Mapping[str, Any]
RecordSequence = Sequence[Record]


@dataclass
class PipelineResult:
    """Structured output returned by pipeline executions."""

    total_records: int
    num_outliers: int
    outlier_ratio: float
    explanations: Sequence[Mapping[str, Any]] = field(default_factory=list)
    metadata: MutableMapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serialisable representation of the result."""
        return {
            "total_records": self.total_records,
            "num_outliers": self.num_outliers,
            "outlier_ratio": self.outlier_ratio,
            "explanations": [dict(explanation) for explanation in self.explanations],
            "metadata": dict(self.metadata),
        }


@dataclass
class CubePipelineResult(PipelineResult):
    """Structured output for cube-based pipelines."""

    cube: Sequence[Mapping[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:  # type: ignore[override]
        payload = super().to_dict()
        payload["cube"] = [dict(row) for row in self.cube]
        return payload


class SimpleThresholdClassifier(Transformer[RecordSequence, Record]):
    """Filter records that exceed a configured threshold."""

    def __init__(self, config: ClassifierConfig) -> None:
        super().__init__()
        self.config = config

    def transform(self, data: RecordSequence) -> Sequence[Record]:  # type: ignore[override]
        outliers: List[Record] = []
        for record in data:
            value = record.get(self.config.metric)
            if value is None:
                continue
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                continue

            threshold = self.config.threshold
            if self.config.greater_is_outlier:
                meets_threshold = (
                    numeric_value >= threshold if self.config.inclusive else numeric_value > threshold
                )
            else:
                meets_threshold = (
                    numeric_value <= threshold if self.config.inclusive else numeric_value < threshold
                )

            if meets_threshold:
                outliers.append(record)

        self._results = tuple(outliers)
        return self._results


class SimpleSummarizer(Transformer[RecordSequence, Mapping[str, Any]]):
    """Aggregate outliers into compact explanations."""

    def __init__(self, config: SummarizerConfig) -> None:
        super().__init__()
        self.config = config

    def _row_weight(self, record: Mapping[str, Any]) -> float:
        if not self.config.weight_column:
            return 1.0
        value = record.get(self.config.weight_column, 0)
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def transform(self, data: RecordSequence) -> Sequence[Mapping[str, Any]]:  # type: ignore[override]
        if not data:
            self._results = tuple()
            return self._results

        totals: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        overall_weight = 0.0

        for record in data:
            weight = self._row_weight(record)
            overall_weight += weight
            key = tuple(record.get(dimension) for dimension in self.config.dimensions)
            if key not in totals:
                totals[key] = {
                    "dimensions": {
                        dimension: record.get(dimension)
                        for dimension in self.config.dimensions
                    },
                    "weight": 0.0,
                    "metrics": {metric: 0.0 for metric in self.config.metrics},
                }
            totals[key]["weight"] += weight
            for metric in self.config.metrics:
                value = record.get(metric)
                if value is None:
                    continue
                try:
                    totals[key]["metrics"][metric] += float(value)
                except (TypeError, ValueError):
                    continue

        results: List[Mapping[str, Any]] = []
        normaliser = overall_weight if overall_weight > 0 else 1.0
        for data_key, aggregate in totals.items():
            support = aggregate["weight"] / normaliser if normaliser else 0.0
            if support < self.config.min_support_ratio:
                continue
            formatted = {
                "dimensions": aggregate["dimensions"],
                "support": support,
                "weight": aggregate["weight"],
                "metrics": aggregate["metrics"],
            }
            results.append(formatted)

        results.sort(key=lambda item: item["support"], reverse=True)
        if self.config.max_results is not None:
            results = results[: self.config.max_results]

        self._results = tuple(results)
        return self._results


class BatchPipeline:
    """Executable pipeline for batch outlier detection and explanation."""

    CLASSIFIERS: Dict[str, Type[Operator]] = {
        "threshold": SimpleThresholdClassifier,
    }
    SUMMARIZERS: Dict[str, Type[Operator]] = {
        "simple": SimpleSummarizer,
    }

    def __init__(self, config: BatchPipelineConfig) -> None:
        self.config = config
        if (
            self.config.record_count_column is None
            and self.config.classifier_config.weight_column is not None
        ):
            self.config.record_count_column = self.config.classifier_config.weight_column
        if (
            self.config.record_count_column
            and self.config.summarizer_config.weight_column is None
        ):
            self.config.summarizer_config.weight_column = self.config.record_count_column
        self.classifier = self._instantiate_operator(
            self.config.classifier, self.config.classifier_config, self.CLASSIFIERS
        )
        self.summarizer = self._instantiate_operator(
            self.config.summarizer, self.config.summarizer_config, self.SUMMARIZERS
        )

    def _instantiate_operator(
        self,
        selector: Union[str, Type[Operator]],
        config: Union[ClassifierConfig, SummarizerConfig],
        registry: Dict[str, Type[Operator]],
    ) -> Operator:
        if isinstance(selector, str):
            if selector not in registry:
                raise KeyError(f"Unknown operator identifier: {selector!r}")
            operator_type = registry[selector]
        else:
            operator_type = selector
        return operator_type(config)  # type: ignore[arg-type]

    def _count_records(self, rows: Iterable[Mapping[str, Any]]) -> int:
        column = self.config.record_count_column
        if column:
            total = 0.0
            for row in rows:
                value = row.get(column, 0)
                try:
                    total += float(value)
                except (TypeError, ValueError):
                    continue
            return int(total)
        if hasattr(rows, "__len__"):
            return len(rows)  # type: ignore[arg-type]
        return sum(1 for _ in rows)

    def run(self, data: RecordSequence) -> PipelineResult:
        """Execute the pipeline over ``data`` and return structured results."""

        total_records = self._count_records(data)
        outliers_sequence = self.classifier.process(data)
        outliers = list(outliers_sequence)
        outlier_count = self._count_records(outliers)

        explanations_sequence = self.summarizer.process(outliers)
        explanations = list(explanations_sequence)
        ratio = (outlier_count / total_records) if total_records else 0.0

        metadata: Dict[str, Any] = {
            "classifier": type(self.classifier).__name__,
            "summarizer": type(self.summarizer).__name__,
        }
        metadata.update(self.config.metadata)
        result = PipelineResult(
            total_records=total_records,
            num_outliers=outlier_count,
            outlier_ratio=ratio,
            explanations=explanations,
            metadata=metadata,
        )
        return result


class CubePipeline:
    """Pipeline variant that first builds a data cube before classification."""

    def __init__(self, config: CubePipelineConfig) -> None:
        self.config = config
        self.batch = BatchPipeline(self.config.batch)

    def _build_cube(self, data: RecordSequence) -> List[Dict[str, Any]]:
        cube_map: Dict[Tuple[Any, ...], Dict[str, Any]] = defaultdict(dict)
        count_column = self.config.record_count_column
        for record in data:
            key = tuple(record.get(dimension) for dimension in self.config.cube_dimensions)
            cube_entry = cube_map.setdefault(
                key,
                {
                    dimension: record.get(dimension)
                    for dimension in self.config.cube_dimensions
                },
            )
            cube_entry.setdefault(count_column, 0.0)
            try:
                increment = float(record.get(count_column, 1.0))
            except (TypeError, ValueError):
                increment = 1.0
            cube_entry[count_column] += increment
            for metric in self.config.metrics:
                cube_entry.setdefault(metric, 0.0)
                value = record.get(metric, 0.0)
                try:
                    cube_entry[metric] += float(value)
                except (TypeError, ValueError):
                    continue
        return [cube_map[key] for key in cube_map]

    def run(self, data: RecordSequence) -> CubePipelineResult:
        cube = self._build_cube(data)
        batch_result = self.batch.run(cube)
        metadata = dict(batch_result.metadata)
        metadata.setdefault("cube_dimensions", list(self.config.cube_dimensions))
        metadata.setdefault("metrics", list(self.config.metrics))
        metadata["original_rows"] = len(data)
        return CubePipelineResult(
            total_records=batch_result.total_records,
            num_outliers=batch_result.num_outliers,
            outlier_ratio=batch_result.outlier_ratio,
            explanations=batch_result.explanations,
            metadata=metadata,
            cube=cube,
        )
