"""Configuration objects for MacroBase Python pipelines."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import MutableMapping, Optional, Sequence, Type, Union

from .base import Operator


@dataclass
class ClassifierConfig:
    """Configuration for simple threshold-based classifiers."""

    metric: str
    threshold: float
    greater_is_outlier: bool = True
    inclusive: bool = True
    weight_column: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.metric:
            raise ValueError("'metric' must be a non-empty string.")
        if not isinstance(self.threshold, (int, float)):
            raise TypeError("'threshold' must be a numeric value.")
        if self.weight_column is not None and not self.weight_column:
            raise ValueError("'weight_column' cannot be an empty string when provided.")


@dataclass
class SummarizerConfig:
    """Configuration for summarising outlier groups."""

    dimensions: Sequence[str] = field(default_factory=list)
    min_support_ratio: float = 0.05
    max_results: Optional[int] = 20
    weight_column: Optional[str] = None
    metrics: Sequence[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.min_support_ratio < 0 or self.min_support_ratio > 1:
            raise ValueError("'min_support_ratio' must be between 0 and 1.")
        if self.max_results is not None and self.max_results <= 0:
            raise ValueError("'max_results' must be positive when provided.")
        if self.weight_column is not None and not self.weight_column:
            raise ValueError("'weight_column' cannot be an empty string when provided.")


@dataclass
class BatchPipelineConfig:
    """Top-level configuration for :class:`~macrobase_py.pipeline.BatchPipeline`."""

    classifier_config: ClassifierConfig
    summarizer_config: SummarizerConfig = field(default_factory=SummarizerConfig)
    classifier: Union[str, Type[Operator]] = "threshold"
    summarizer: Union[str, Type[Operator]] = "simple"
    record_count_column: Optional[str] = None
    metadata: MutableMapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.classifier_config, ClassifierConfig):
            raise TypeError("'classifier_config' must be a ClassifierConfig instance.")
        if not isinstance(self.summarizer_config, SummarizerConfig):
            raise TypeError("'summarizer_config' must be a SummarizerConfig instance.")
        if isinstance(self.classifier, str) and not self.classifier:
            raise ValueError("'classifier' string identifier cannot be empty.")
        if isinstance(self.summarizer, str) and not self.summarizer:
            raise ValueError("'summarizer' string identifier cannot be empty.")
        if self.record_count_column is not None and not self.record_count_column:
            raise ValueError("'record_count_column' cannot be an empty string when provided.")


@dataclass
class CubePipelineConfig:
    """Configuration for :class:`~macrobase_py.pipeline.CubePipeline`."""

    batch: BatchPipelineConfig
    cube_dimensions: Sequence[str]
    metrics: Sequence[str]
    record_count_column: str = "count"

    def __post_init__(self) -> None:
        if not isinstance(self.batch, BatchPipelineConfig):
            raise TypeError("'batch' must be a BatchPipelineConfig instance.")
        if not self.cube_dimensions:
            raise ValueError("'cube_dimensions' must contain at least one dimension.")
        if not self.metrics:
            raise ValueError("'metrics' must contain at least one metric to aggregate.")
        if not self.record_count_column:
            raise ValueError("'record_count_column' must be a non-empty string.")
        if self.batch.record_count_column is None:
            self.batch.record_count_column = self.record_count_column
        if not self.batch.summarizer_config.dimensions:
            self.batch.summarizer_config.dimensions = self.cube_dimensions
        if self.batch.summarizer_config.weight_column is None:
            self.batch.summarizer_config.weight_column = self.record_count_column
