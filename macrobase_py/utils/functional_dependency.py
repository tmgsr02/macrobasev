"""Functional dependency utilities."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, Mapping, MutableMapping, Optional, Sequence, Set, Tuple


Record = Mapping[str, object]


@dataclass(frozen=True)
class FunctionalDependencyResult:
    """Outcome of a functional dependency check."""

    determinants: Tuple[str, ...]
    dependents: Tuple[str, ...]
    total_rows: int
    violating_rows: int
    conflicts: Mapping[Tuple[object, ...], Tuple[Tuple[object, ...], ...]]

    @property
    def holds(self) -> bool:
        return self.violating_rows == 0

    @property
    def violation_ratio(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return self.violating_rows / self.total_rows


def _iter_records(
    data: Iterable[object],
    *,
    columns: Optional[Sequence[str]] = None,
) -> Iterator[Record]:
    if hasattr(data, "to_dict") and hasattr(data, "columns"):
        # pandas DataFrame like object
        for record in data.to_dict("records"):
            yield record
        return

    for row in data:
        if isinstance(row, Mapping):
            yield row
        elif columns is not None:
            if not isinstance(row, Sequence):
                raise TypeError("Row must be a sequence when columns are provided")
            if len(columns) != len(row):
                raise ValueError("Row length does not match number of columns")
            yield {col: value for col, value in zip(columns, row)}
        else:
            raise TypeError(
                "Rows must be mappings unless column names are provided via 'columns'"
            )


def check_functional_dependency(
    data: Iterable[object],
    determinants: Sequence[str],
    dependents: Sequence[str],
    *,
    columns: Optional[Sequence[str]] = None,
    ignore_nulls: bool = False,
) -> FunctionalDependencyResult:
    """Check if ``determinants`` functionally determine ``dependents``.

    ``data`` can be an iterable of mappings, sequences accompanied by
    ``columns`` or a pandas ``DataFrame``.  ``ignore_nulls`` controls whether
    rows containing ``None`` for any participating column should be ignored.
    """

    determinant_tuple = tuple(determinants)
    dependent_tuple = tuple(dependents)

    key_to_value: Dict[Tuple[object, ...], Tuple[object, ...]] = {}
    key_to_rows: Dict[Tuple[object, ...], Set[int]] = {}
    conflicts: Dict[Tuple[object, ...], Set[Tuple[object, ...]]] = {}
    violating_rows: Set[int] = set()
    total_rows = 0

    for idx, record in enumerate(_iter_records(data, columns=columns)):
        try:
            key = tuple(record[column] for column in determinant_tuple)
            value = tuple(record[column] for column in dependent_tuple)
        except KeyError as exc:  # pragma: no cover - defensive path
            missing = exc.args[0]
            raise KeyError(f"Missing column '{missing}' in row {idx}") from None

        if ignore_nulls and (None in key or None in value):
            continue

        total_rows += 1

        if key not in key_to_value:
            key_to_value[key] = value
            key_to_rows[key] = {idx}
            continue

        if key_to_value[key] == value:
            key_to_rows[key].add(idx)
            continue

        conflicts.setdefault(key, set()).update({key_to_value[key], value})
        key_to_rows[key].add(idx)
        violating_rows.update(key_to_rows[key])
        key_to_value[key] = value

    frozen_conflicts: Dict[Tuple[object, ...], Tuple[Tuple[object, ...], ...]] = {
        key: tuple(sorted(values)) for key, values in conflicts.items()
    }

    return FunctionalDependencyResult(
        determinants=determinant_tuple,
        dependents=dependent_tuple,
        total_rows=total_rows,
        violating_rows=len(violating_rows),
        conflicts=frozen_conflicts,
    )


def dependency_summary(
    results: Iterable[FunctionalDependencyResult],
    *,
    max_ratio: Optional[float] = None,
    max_violations: Optional[int] = None,
) -> Dict[str, bool]:
    """Return a mapping describing which dependencies satisfy the thresholds."""

    summary: Dict[str, bool] = {}
    for result in results:
        satisfies = result.holds
        if max_ratio is not None and result.violation_ratio > max_ratio:
            satisfies = False
        if max_violations is not None and result.violating_rows > max_violations:
            satisfies = False
        key = " ,".join(result.determinants) + " -> " + " ,".join(result.dependents)
        summary[key] = satisfies
    return summary
