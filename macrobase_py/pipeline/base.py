"""Core abstract base classes for MacroBase Python pipelines."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, Optional, Sequence, TypeVar

InputT = TypeVar("InputT")
ResultT = TypeVar("ResultT")


class Operator(ABC, Generic[InputT, ResultT]):
    """Base class for units of computation within a pipeline."""

    def __init__(self) -> None:
        self._results: Optional[Sequence[ResultT]] = None

    @abstractmethod
    def process(self, data: InputT) -> Sequence[ResultT]:
        """Consume ``data`` and persist the operator's results."""

    def get_results(self) -> Sequence[ResultT]:
        """Return the results from the most recent :meth:`process` call."""
        if self._results is None:
            raise RuntimeError(
                "Operator results are not available. Call process() before get_results()."
            )
        return self._results


class Transformer(Operator[InputT, ResultT], ABC):
    """Specialised :class:`Operator` that exposes a ``transform`` helper."""

    @abstractmethod
    def transform(self, data: InputT) -> Sequence[ResultT]:
        """Produce transformed output for ``data``."""

    def process(self, data: InputT) -> Sequence[ResultT]:  # type: ignore[override]
        self._results = self.transform(data)
        return self._results
